package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.ClickhouseDbProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class ClickhouseConfig {

    @Autowired
    private ClickhouseDbProperties clickhouseDbProperties;

    @Bean
    public ClickHouseDataSource clickHouseDataSource() {
        Properties info = new Properties();
        info.setProperty(ClickHouseQueryParam.USER.getKey(), clickhouseDbProperties.getUser());
        info.setProperty(ClickHouseQueryParam.PASSWORD.getKey(), clickhouseDbProperties.getPassword());
        info.setProperty(ClickHouseQueryParam.COMPRESS.getKey(), String.valueOf(clickhouseDbProperties.getCompress()));
        info.setProperty(ClickHouseQueryParam.CONNECT_TIMEOUT.getKey(), String.valueOf(clickhouseDbProperties.getConnectionTimeout()));
        return new ClickHouseDataSource(clickhouseDbProperties.getUrl(), info);
    }

    @Bean
    @Autowired
    public JdbcTemplate jdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }

}
