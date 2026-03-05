package dev.vality.analytics.listener;

import dev.vality.analytics.config.SpringBootITest;
import dev.vality.analytics.utils.WithdrawalEventTestUtils;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootITest
public class WithdrawalListenerTest {

    @Value("${kafka.topic.withdrawal.initial}")
    private String withdrawalTopic;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;
    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;
    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void shouldReduceWithdrawalStateAndWriteClickHouseSnapshot() {
        seedShopDictionary();

        List<TBase<?, ?>> flow = WithdrawalEventTestUtils.fullSuccessFlow();
        flow.forEach(event -> testThriftKafkaProducer.send(withdrawalTopic, event));

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Integer count = clickHouseJdbcTemplate.queryForObject(
                    "SELECT count(*) FROM analytic.events_sink_withdrawal WHERE withdrawalId = ?",
                    Integer.class,
                    WithdrawalEventTestUtils.WITHDRAWAL_ID);
            return count != null && count == 1;
        });

        Map<String, Object> stateRow = postgresJdbcTemplate.queryForMap(
                "SELECT * FROM analytics.withdrawal_state WHERE withdrawal_id = ?",
                WithdrawalEventTestUtils.WITHDRAWAL_ID);
        assertEquals(WithdrawalEventTestUtils.PARTY_ID, stateRow.get("party_id"));
        assertEquals(WithdrawalEventTestUtils.CURRENCY, stateRow.get("currency"));
        assertEquals(1200L, ((Number) stateRow.get("amount")).longValue());
        assertEquals(100L, ((Number) stateRow.get("system_fee")).longValue());
        assertEquals(20L, ((Number) stateRow.get("provider_fee")).longValue());
        assertEquals("42", stateRow.get("provider_id"));
        assertEquals("24", stateRow.get("terminal"));

        Map<String, Object> withdrawalRow = clickHouseJdbcTemplate.queryForMap(
                "SELECT partyId, currency, providerId, terminal, amount, systemFee, providerFee, status " +
                        "FROM analytic.events_sink_withdrawal WHERE withdrawalId = ? AND status = 'succeeded'",
                WithdrawalEventTestUtils.WITHDRAWAL_ID);
        assertEquals(WithdrawalEventTestUtils.PARTY_ID, withdrawalRow.get("partyId"));
        assertEquals(WithdrawalEventTestUtils.CURRENCY, withdrawalRow.get("currency"));
        assertEquals("42", withdrawalRow.get("providerId"));
        assertEquals("24", withdrawalRow.get("terminal"));
        assertEquals(1200L, ((Number) withdrawalRow.get("amount")).longValue());
        assertEquals(100L, ((Number) withdrawalRow.get("systemFee")).longValue());
        assertEquals(20L, ((Number) withdrawalRow.get("providerFee")).longValue());

        clickHouseJdbcTemplate.execute("SYSTEM RELOAD DICTIONARY analytic.shop_dictionary");
        String locationUrl = clickHouseJdbcTemplate.queryForObject(
                "SELECT dictGet('analytic.shop_dictionary', 'location_url', tuple(?, ?))",
                String.class,
                WithdrawalEventTestUtils.PARTY_ID,
                "shop-dict-1");
        assertEquals("https://merchant.example/shop-dict-1", locationUrl);
    }

    private void seedShopDictionary() {
        postgresJdbcTemplate.update(
                "INSERT INTO analytics.shop " +
                        "(event_id, event_time, party_id, shop_id, location_url) " +
                        "VALUES (?, ?, ?, ?, ?)",
                1L,
                LocalDateTime.of(2024, 1, 10, 10, 0),
                WithdrawalEventTestUtils.PARTY_ID,
                "shop-dict-1",
                "https://merchant.example/shop-dict-1");
    }
}
