package dev.vality.analytics.config;

import dev.vality.analytics.config.properties.ClickHouseDbProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class ClickHouseConfig {

    @Bean(name = "clickHouseDataSourceProperties")
    @ConfigurationProperties(prefix = "clickhouse.db")
    public ClickHouseDbProperties clickHouseDataSourceProperties() {
        return new ClickHouseDbProperties();
    }

    @Bean(name = "clickHouseDataSource")
    @ConfigurationProperties(prefix = "clickhouse.db.hikari")
    public DataSource clickHouseDataSource(@Qualifier("clickHouseDataSourceProperties") ClickHouseDbProperties clickHouseDataSourceProperties) {
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
}
