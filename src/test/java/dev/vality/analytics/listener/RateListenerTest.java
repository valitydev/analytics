package dev.vality.analytics.listener;

import dev.vality.analytics.config.SpringBootITest;
import dev.vality.analytics.utils.RateSinkEventTestUtils;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
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
    public void handle() throws InterruptedException {
        String sourceId = "CBR";

        final List<SinkEvent> sinkEvents = RateSinkEventTestUtils.create(sourceId);
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(rateTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Integer count = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.rate", Integer.class);
            if (count == 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });
        final List<Map<String, Object>> maps = postgresJdbcTemplate.queryForList("SELECT * FROM analytics.rate");

        assertEquals(4, maps.size());
    }
}
