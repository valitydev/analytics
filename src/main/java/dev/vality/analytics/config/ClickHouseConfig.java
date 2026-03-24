package dev.vality.analytics.config;

import dev.vality.analytics.config.properties.ClickHouseDbProperties;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Configuration
public class ClickHouseConfig {

    private static final String NON_SHARDED_SCHEMA_MODE = "non-sharded";
    private static final String REPLICATED_NON_SHARDED_SCHEMA_MODE = "replicated-non-sharded";
    private static final String SHARDED_SCHEMA_MODE = "sharded";
    private static final String NON_SHARDED_MIGRATION_LOCATION = "classpath:db/migration-clickhouse/non-sharded";
    private static final String REPLICATED_NON_SHARDED_MIGRATION_LOCATION =
            "classpath:db/migration-clickhouse/replicated-non-sharded";
    private static final String SHARDED_MIGRATION_LOCATION = "classpath:db/migration-clickhouse/sharded";

    @Bean(name = "clickHouseDataSourceProperties")
    @ConfigurationProperties(prefix = "clickhouse.db")
    public ClickHouseDbProperties clickHouseDataSourceProperties() {
        return new ClickHouseDbProperties();
    }

    @Bean(name = "clickHouseDataSource")
    @ConfigurationProperties(prefix = "clickhouse.db.hikari")
    public DataSource clickHouseDataSource(
            @Qualifier("clickHouseDataSourceProperties") ClickHouseDbProperties clickHouseDataSourceProperties) {
        clickHouseDataSourceProperties.setUrl(buildClickHouseJdbcUrl(clickHouseDataSourceProperties));
        return clickHouseDataSourceProperties
                .initializeDataSourceBuilder()
                .type(com.zaxxer.hikari.HikariDataSource.class)
                .build();
    }

    @Bean(name = "clickHouseJdbcTemplate")
    public JdbcTemplate clickHouseJdbcTemplate(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }

    @Bean
    @DependsOn("flyway")
    @ConditionalOnProperty(prefix = "clickhouse.flyway", name = "enabled", havingValue = "true", matchIfMissing = true)
    public Flyway clickHouseFlyway(
            @Qualifier("clickHouseDataSource") DataSource clickHouseDataSource,
            @Qualifier("dataSourceProperties") DataSourceProperties postgresDataSourceProperties,
            @Value("${clickhouse.flyway.schema-mode:non-sharded}") String schemaMode,
            @Value("${postgres.db.schema}") String postgresSchema) {
        String normalizedSchemaMode = schemaMode.toLowerCase(Locale.ROOT);
        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.putAll(ClickHouseFlywaySupport.resolvePostgresPlaceholders(
                postgresDataSourceProperties.getUrl(),
                postgresDataSourceProperties.getUsername(),
                postgresDataSourceProperties.getPassword(),
                postgresSchema));
        final var flyway = ClickHouseFlywaySupport.createFlyway(
                clickHouseDataSource,
                List.of(resolveClickHouseMigrationLocation(normalizedSchemaMode)),
                placeholders,
                "clickhouse_flyway_schema_history");
        flyway.migrate();
        return flyway;
    }

    private String buildClickHouseJdbcUrl(ClickHouseDbProperties properties) {
        var urlBuilder = new StringBuilder();
        urlBuilder.append(properties.getUrl());
        var hasParams = false;
        if (properties.getCompress() != null) {
            urlBuilder.append("?");
            urlBuilder.append("compress=").append(properties.getCompress());
            hasParams = true;
        }
        if (properties.getConnectionTimeout() != null) {
            urlBuilder.append(hasParams ? "&" : "?");
            urlBuilder.append("connect_timeout=").append(properties.getConnectionTimeout());
        }
        return urlBuilder.toString();
    }

    private String resolveClickHouseMigrationLocation(String schemaMode) {
        return switch (schemaMode) {
            case NON_SHARDED_SCHEMA_MODE -> NON_SHARDED_MIGRATION_LOCATION;
            case REPLICATED_NON_SHARDED_SCHEMA_MODE -> REPLICATED_NON_SHARDED_MIGRATION_LOCATION;
            case SHARDED_SCHEMA_MODE -> SHARDED_MIGRATION_LOCATION;
            default -> throw new IllegalArgumentException(
                    String.format("Unsupported clickhouse.flyway.schema-mode: %s. Allowed values: %s, %s, %s",
                            schemaMode,
                            NON_SHARDED_SCHEMA_MODE,
                            REPLICATED_NON_SHARDED_SCHEMA_MODE,
                            SHARDED_SCHEMA_MODE));
        };
    }
}
