package dev.vality.analytics.repository;

import dev.vality.analytics.config.ClickHouseConfig;
import dev.vality.analytics.config.properties.ClickHouseDbProperties;
import dev.vality.clickhouse.initializer.ChInitializer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.util.List;

@Slf4j
@Testcontainers
@ContextConfiguration(initializers = ClickHouseAbstractTest.Initializer.class,
        classes = {JdbcTemplateAutoConfiguration.class, ClickHouseDbProperties.class, ClickHouseConfig.class})
public class ClickHouseAbstractTest {

    @Container
    public static ClickHouseContainer clickHouseContainer =
            new ClickHouseContainer("clickhouse/clickhouse-server:22.3")
                    .withCommand("--config-file=/etc/clickhouse-server/config.xml")
                    .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/arm64"));

    @BeforeEach
    public void init() throws SQLException {
        ChInitializer.initAllScripts(clickHouseContainer, List.of(
                "sql/V1__db_init.sql",
                "sql/V2__add_fields.sql",
                "sql/V3__add_provider_field.sql",
                "sql/test.data/inserts_event_sink.sql")
        );
    }

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
}
