package dev.vality.analytics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

@Getter
@Setter
public class ClickHouseDbProperties extends DataSourceProperties {

    private Long connectionTimeout;
    private Boolean compress;

}
