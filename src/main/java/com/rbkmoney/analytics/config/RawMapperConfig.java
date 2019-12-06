package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.converter.RawToCostConverter;
import com.rbkmoney.analytics.converter.RawToCountModelConverter;
import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.dao.mapper.CommonRowsMapper;
import com.rbkmoney.analytics.dao.model.Cost;
import com.rbkmoney.analytics.dao.model.CountModel;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RawMapperConfig {

    @Bean
    public CommonRowsMapper<Cost> costCommonRowsMapper(RawToCostConverter rawToCostConverter) {
        return new CommonRowsMapper<>(rawToCostConverter);
    }

    @Bean
    public CommonRowsMapper<CountModel> countModelCommonRowsMapper(RawToCountModelConverter rawToCountModelConverter) {
        return new CommonRowsMapper<>(rawToCountModelConverter);
    }

    @Bean
    public CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper(RawToNamingDistributionConverter rawToNamingDistributionConverter) {
        return new CommonRowsMapper<>(rawToNamingDistributionConverter);
    }

}
