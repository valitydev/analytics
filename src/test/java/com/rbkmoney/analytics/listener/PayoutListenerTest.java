package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.damsel.domain.CurrencyRef;
import com.rbkmoney.damsel.geo_ip.GeoIpServiceSrv;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.payout.manager.*;
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
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

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
    private GeoIpServiceSrv.Iface iface;
    @MockBean
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;
    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Before
    public void init() throws SQLException {
        try (Connection connection = getSystemConn()) {
            String sql = FileUtil.getFile("sql/V1__db_init.sql") + ";" +
                    FileUtil.getFile("sql/V7__new_payouts.sql");
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
    public void testPayout() throws InterruptedException, TException {
        // Given
        Event payoutEvent = new Event()
                .setPayoutChange(
                        PayoutChange.status_changed(new PayoutStatusChanged()
                                .setStatus(PayoutStatus.paid(new PayoutPaid()))))
                .setPayoutId(PAYOUT_ID)
                .setCreatedAt(TypeUtil.temporalToString(Instant.now()))
                .setPayout(new Payout()
                        .setShopId(SHOP_ID)
                        .setAmount(10L)
                        .setCurrency(new CurrencyRef()
                                .setSymbolicCode("RUB"))
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
