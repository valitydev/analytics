package dev.vality.analytics.config;

import lombok.experimental.UtilityClass;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
final class ClickHouseFlywaySupport {

    private static final String POSTGRES_HOST = "postgresHost";
    private static final String POSTGRES_PORT = "postgresPort";
    private static final String POSTGRES_USER = "postgresUser";
    private static final String POSTGRES_PASSWORD = "postgresPassword";
    private static final String POSTGRES_DATABASE = "postgresDatabase";
    private static final String POSTGRES_SCHEMA = "postgresSchema";
    private static final Pattern POSTGRES_JDBC_URL_PATTERN = Pattern.compile(
            "^jdbc:postgresql://(?<host>[^:/?]+)(?::(?<port>\\d+))?/(?<database>[^?]+).*$");

    static Flyway createFlyway(
            DataSource dataSource,
            List<String> locations,
            Map<String, String> placeholders,
            String schemaHistoryTable) {
        return Flyway.configure()
                .dataSource(dataSource)
                .locations(locations.toArray(String[]::new))
                .placeholderPrefix("<<")
                .placeholderSuffix(">>")
                .placeholders(placeholders)
                .table(schemaHistoryTable)
                .baselineOnMigrate(true)
                .load();
    }

    static Map<String, String> resolvePostgresPlaceholders(
            String jdbcUrl,
            String username,
            String password,
            String schema) {
        Matcher matcher = POSTGRES_JDBC_URL_PATTERN.matcher(jdbcUrl);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unsupported postgres.db.url format: " + jdbcUrl);
        }

        String port = matcher.group("port");
        return resolvePostgresPlaceholders(
                matcher.group("host"),
                port == null ? 5432 : Integer.parseInt(port),
                matcher.group("database"),
                username,
                password,
                schema);
    }

    static Map<String, String> resolvePostgresPlaceholders(
            String host,
            int port,
            String database,
            String username,
            String password,
            String schema) {
        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.put(POSTGRES_HOST, host);
        placeholders.put(POSTGRES_PORT, Integer.toString(port));
        placeholders.put(POSTGRES_USER, username);
        placeholders.put(POSTGRES_PASSWORD, password);
        placeholders.put(POSTGRES_DATABASE, database);
        placeholders.put(POSTGRES_SCHEMA, schema);
        return placeholders;
    }
}
