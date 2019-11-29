package com.rbkmoney.analytics;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.analytics.serde.MgEventSinkRowDeserializer;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = EventSinkListenerTest.Initializer.class)
public class EventSinkListenerTest extends KafkaAbstractTest {

    @ClassRule
    public static ClickHouseContainer clickHouseContainer = new ClickHouseContainer();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            log.info("clickhouse.db.url={}", clickHouseContainer.getJdbcUrl());
            TestPropertyValues.of("clickhouse.db.url=" + clickHouseContainer.getJdbcUrl(),
                    "clickhouse.db.user=" + clickHouseContainer.getUsername(),
                    "clickhouse.db.password=" + clickHouseContainer.getPassword())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Before
    public void init() throws SQLException {
        try (Connection connection = getSystemConn()) {
            String sql = FileUtil.getFile("sql/V1__db_init.sql");
            String[] split = sql.split(";");
            for (String exec : split) {
                connection.createStatement().execute(exec);
            }
        }
    }

    private Connection getSystemConn() throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickHouseContainer.getJdbcUrl(), properties);
        return dataSource.getConnection();
    }

    @Test
    public void testEventSink() throws InterruptedException {
        List<SinkEvent> sinkEvents = MgEventSinkFlowGenerator.generateSuccessFlow("sourceID");
        sinkEvents.forEach(this::produceMessageToEventSink);

        sinkEvents = MgEventSinkFlowGenerator.generateSuccessNotFullFlow("sourceID_2");
        sinkEvents.forEach(this::produceMessageToEventSink);

        Thread.sleep(4_000l);

        Consumer<String, MgEventSinkRow> consumer = createConsumer(MgEventSinkRowDeserializer.class);
        consumer.subscribe(Arrays.asList(AGGREGATED_EVENT_SINK));

        Thread.sleep(2_000l);

        //check sum for captured payment
        long sum = jdbcTemplate.queryForObject(
                "SELECT shopId, sum(amount) as sum " +
                        "from analytic.events_sink " +
                        "group by shopId, currency, status " +
                        "having shopId = '" + MgEventSinkFlowGenerator.SHOP_ID + "' and status = 'captured' and currency = 'RUB' AND sum(sign) > 0",
                (resultSet, i) -> resultSet.getLong("sum"));

        Assert.assertEquals(12L, sum);

        //statistic for paymentTool
        List<Map<String, Object>> list = jdbcTemplate.queryForList(
                "SELECT shopId, paymentTool," +
                        "( SELECT sum(sign) from analytic.events_sink " +
                        "group by shopId, currency " +
                        "having shopId = '" + MgEventSinkFlowGenerator.SHOP_ID + "' and currency = 'RUB' " +
                        "AND sum(sign) > 0) as total_count, " +
                        "sum(sign) * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by shopId, currency, paymentTool " +
                        "having shopId = '" + MgEventSinkFlowGenerator.SHOP_ID + "' and currency = 'RUB' " +
                        "AND sum(sign) > 0");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    Assert.assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );

    }


    @AfterClass
    public static void clean() {
        File dir = new File("tmp/state-store/analytics-mg-event-sink/1_0/rocksdb/KSTREAM-AGGREGATE-STATE-STORE-0000000004");
        for (File file : dir.listFiles()) {
            file.delete();
        }
        dir.delete();
    }
}
