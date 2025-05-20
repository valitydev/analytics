package dev.vality.analytics.config;

import dev.vality.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@KafkaTestcontainerSingleton(
        properties = {
                "kafka.state.cache.size=0"},
        topicsKeys = {
                "kafka.topic.event.sink.initial",
                "kafka.topic.party.initial",
                "kafka.topic.rate.initial"})
public @interface KafkaTest {
}
