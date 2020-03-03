package com.rbkmoney.analytics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Configuration
@ConfigurationProperties("clickhouse.db")
@Validated
@EnableConfigurationProperties
public class ClickHouseDbProperties {

    private String url;
    private String user;
    private String password;
    private Long connectionTimeout;
    private Boolean compress;

}
