package dev.vality.analytics.listener;

import dev.vality.analytics.AnalyticsApplication;
import dev.vality.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import dev.vality.analytics.utils.KafkaAbstractTest;
import dev.vality.clickhouse.initializer.ChInitializer;
import dev.vality.columbus.ColumbusServiceSrv;
import dev.vality.damsel.domain.CurrencyRef;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.payout.manager.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = PayoutListenerTest.Initializer.class)
public class PayoutListenerTest extends KafkaAbstractTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;
    private static final String SHOP_ID = "SHOP_ID";
    private static final String PAYOUT_ID = "PAYOUT_ID";
    private static final String SELECT_SUM = "SELECT shopId, sum(amount) as sum " +
            "from %1s " +
            "group by shopId, currency, status " +
            "having shopId = '";

    @ClassRule
    public static ClickHouseContainer clickHouseContainer = new ClickHouseContainer();
    @MockBean
    private ColumbusServiceSrv.Iface iface;
    @MockBean
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;
    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Before
    public void init() throws SQLException {
        ChInitializer.initAllScripts(clickHouseContainer, List.of(
                "sql/V1__db_init.sql",
                "sql/V7__new_payouts.sql"
        ));
    }

    @Test
    public void testPayout() throws InterruptedException, TException {
        // Given
        Event payoutEvent = new Event()
                .setPayoutChange(
                        PayoutChange.status_changed(new PayoutStatusChanged()
                                .setStatus(PayoutStatus.paid(new PayoutPaid()))))
                .setPayoutId(PAYOUT_ID)
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                .setPayout(new Payout()
                        .setPayoutId(PAYOUT_ID)
                        .setPartyId("1")
                        .setShopId(SHOP_ID)
                        .setStatus(PayoutStatus.paid(new PayoutPaid()))
                        .setPayoutToolId("111")
                        .setFee(0L)
                        .setAmount(10L)
                        .setCurrency(new CurrencyRef()
                                .setSymbolicCode("RUB"))
                        .setCashFlow(Collections.emptyList())
                        .setCreatedAt(TypeUtil.temporalToString(Instant.now())));

        // When
        produceMessageToPayout(payoutEvent);
        Thread.sleep(MESSAGE_TIMEOUT);

        // Then
        long sum = clickHouseJdbcTemplate.queryForObject(
                String.format(SELECT_SUM, "analytic.events_sink_payout") + SHOP_ID +
                        "' and status = 'paid' and currency = 'RUB'",
                (resultSet, i) -> resultSet.getLong("sum"));

        assertEquals(10L, sum);
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            log.info("clickhouse.db.url={}", clickHouseContainer.getJdbcUrl());
            TestPropertyValues.of(
                    "clickhouse.db.url=" + clickHouseContainer.getJdbcUrl(),
                    "clickhouse.db.user=" + clickHouseContainer.getUsername(),
                    "clickhouse.db.password=" + clickHouseContainer.getPassword(),
                    "spring.flyway.enabled=false")
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
