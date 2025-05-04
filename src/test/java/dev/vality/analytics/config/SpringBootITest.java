package dev.vality.analytics.config;


import dev.vality.testcontainers.annotations.KafkaConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ClickhouseTest
@PostgresqlTest
@KafkaTest
@KafkaConfig
public @interface SpringBootITest {
}
