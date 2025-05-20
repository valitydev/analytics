package dev.vality.analytics.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Component
@Validated
@ConfigurationProperties(prefix = "kafka.streams.payment")
public class PaymentStreamProperties {

    private boolean enabled;
    private boolean cleanInstall;
    private boolean throttlingEnabled;
    private int throttlingTimeoutMs;
    private String initialEventSink;
    private String aggregatedSinkTopic;

}
