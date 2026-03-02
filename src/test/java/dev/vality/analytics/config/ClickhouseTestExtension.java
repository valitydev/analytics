package dev.vality.analytics.config;

import dev.vality.testcontainers.annotations.postgresql.PostgresqlContainerExtension;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.Testcontainers;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

public class ClickhouseTestExtension implements BeforeAllCallback, BeforeEachCallback {

    private static final String ANALYTIC_DB = "analytic";
    private static final int POSTGRES_PORT = 5432;
    private static final String POSTGRES_SCHEMA = "analytics";
    private static final String CLICKHOUSE_MIGRATION_LOCATION = "classpath:db/migration-clickhouse/non-sharded";
    private static final String CLICKHOUSE_TEST_MIGRATION_LOCATION = "classpath:db/test-migration-clickhouse";
    private static final String TESTCONTAINERS_POSTGRES_HOST = "host.testcontainers.internal";

    @Override
    public void beforeAll(ExtensionContext context) {
        var postgres = PostgresqlTestcontainerFactory.singletonContainer();
        if (!postgres.isRunning()) {
            postgres.start();
        }
        Testcontainers.exposeHostPorts(postgres.getMappedPort(POSTGRES_PORT));

        var container = ClickhouseTestContainerHolder.getContainer();
        System.setProperty("clickhouse.db.url", buildClickHouseDatabaseUrl(container.getJdbcUrl()));
        System.setProperty("clickhouse.db.username", container.getUsername());
        System.setProperty("clickhouse.db.password", container.getPassword());
        System.setProperty("clickhouse.flyway.enabled", "false");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        var postgres = PostgresqlTestcontainerFactory.singletonContainer();
        migratePostgres(postgres);

        var container = ClickhouseTestContainerHolder.getContainer();
        try (var connection = DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword())) {
            try (var statement = connection.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS " + ANALYTIC_DB);
                statement.execute("DROP TABLE IF EXISTS clickhouse_flyway_schema_history");
            }
        }
        migrateClickHouse(container, postgres);
    }

    private void migratePostgres(PostgresqlContainerExtension postgres) {
        Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .schemas(POSTGRES_SCHEMA)
                .load()
                .migrate();
    }

    private void migrateClickHouse(ClickHouseContainer clickhouse, PostgresqlContainerExtension postgres) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                clickhouse.getJdbcUrl(),
                clickhouse.getUsername(),
                clickhouse.getPassword());
        Map<String, String> placeholders = ClickHouseFlywaySupport.resolvePostgresPlaceholders(
                TESTCONTAINERS_POSTGRES_HOST,
                postgres.getMappedPort(POSTGRES_PORT),
                postgres.getDatabaseName(),
                postgres.getUsername(),
                postgres.getPassword(),
                POSTGRES_SCHEMA);
        Flyway flyway = ClickHouseFlywaySupport.createFlyway(
                dataSource,
                List.of(CLICKHOUSE_MIGRATION_LOCATION, CLICKHOUSE_TEST_MIGRATION_LOCATION),
                placeholders,
                "clickhouse_flyway_schema_history");
        flyway.migrate();
    }

    private String buildClickHouseDatabaseUrl(String jdbcUrl) {
        int paramsStart = jdbcUrl.indexOf('?');
        String base = paramsStart >= 0 ? jdbcUrl.substring(0, paramsStart) : jdbcUrl;
        String params = paramsStart >= 0 ? jdbcUrl.substring(paramsStart) : "";
        int databaseSeparator = base.lastIndexOf('/');
        if (databaseSeparator < 0) {
            throw new IllegalArgumentException("Unsupported ClickHouse JDBC URL: " + jdbcUrl);
        }
        return base.substring(0, databaseSeparator + 1) + ANALYTIC_DB + params;
    }
}
