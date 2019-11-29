package com.rbkmoney.analytics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Configuration
@ConfigurationProperties("kafka.ssl")
@Validated
public class KafkaSslProperties {

    private String serverStorePassword;
    private String serverStoreCertPath;
    private String keyStorePassword;
    private String keyPassword;
    private String clientStoreCertPath;
    private boolean kafkaSslEnable;

}
