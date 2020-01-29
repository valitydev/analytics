package com.rbkmoney.analytics.config;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class RocksDBConfig implements RocksDBConfigSetter {

    @Value("${kafka.block.cache.size:10}")
    private int blockCacheSizeMb;

    @Override
    public void setConfig(
            String storeName,
            Options options,
            Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCacheSize(blockCacheSizeMb * 1024 * 1024L);
        tableConfig.setCacheIndexAndFilterBlocks(true);

        options.setTableFormatConfig(tableConfig);
    }
}
