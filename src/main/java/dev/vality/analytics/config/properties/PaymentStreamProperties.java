package dev.vality.analytics.config.properties;

import dev.vality.mg.event.sink.model.CustomProperties;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "kafka.streams.payment")
public class PaymentStreamProperties extends CustomProperties {

    private boolean enabled;

}
