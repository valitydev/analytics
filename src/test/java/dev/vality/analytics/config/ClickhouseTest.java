package dev.vality.analytics.config;

import dev.vality.testcontainers.annotations.clickhouse.ClickhouseTestcontainerSingleton;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ClickhouseTestcontainerSingleton(
        dbNameShouldBeDropped = "analytic",
        migrations = {
                "sql/V1__db_init.sql",
                "sql/V2__add_fields.sql",
                "sql/V3__add_provider_field.sql",
                "sql/test.data/inserts_event_sink.sql"})
public @interface ClickhouseTest {
}
