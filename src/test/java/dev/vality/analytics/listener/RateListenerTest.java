package dev.vality.analytics.listener;

import dev.vality.analytics.config.SpringBootITest;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootITest
public class RateListenerTest {

    @Value("${kafka.topic.rate.initial}")
    public String rateTopic;
    @Autowired
    private JdbcTemplate postgresJdbcTemplate;
    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void handle() {
        List<dev.vality.exrates.events.CurrencyEvent> currencyEvents = RateEventTestUtils.createCurrencyEvents(
                4, Instant.now());

        currencyEvents.forEach(event -> testThriftKafkaProducer.send(rateTopic, event));

        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Integer count = postgresJdbcTemplate.queryForObject(
                    "SELECT count(*) FROM analytics.rate", Integer.class);
            if (count < 4) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });

        final List<Map<String, Object>> maps = postgresJdbcTemplate.queryForList("SELECT * FROM analytics.rate");
        assertEquals(4, maps.size());

        maps.forEach(row -> {
            assertEquals("USD", row.get("source_symbolic_code"));
            assertEquals("RUB", row.get("destination_symbolic_code"));
            assertEquals(2, row.get("source_exponent"));
            assertEquals(2, row.get("destination_exponent"));
        });
    }
}
