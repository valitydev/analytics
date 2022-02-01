package dev.vality.analytics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Configuration
@ConfigurationProperties("postgres.db")
@Validated
public class PostgresDbProperties {

    private String url;
    private String user;
    private String password;
    private String schema;
}
