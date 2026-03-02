package dev.vality.analytics.config;

import org.testcontainers.clickhouse.ClickHouseContainer;

final class ClickhouseTestContainerHolder {

    private static final String IMAGE =
            System.getProperty("clickhouse.test.image", "clickhouse/clickhouse-server:23.10.3");

    private static final ClickHouseContainer CONTAINER = new ClickHouseContainer(IMAGE)
            .withAccessToHost(true)
            .withUsername("test")
            .withPassword("test")
            .withDatabaseName("default");

    private ClickhouseTestContainerHolder() {
    }

    static synchronized ClickHouseContainer getContainer() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }
        return CONTAINER;
    }
}
