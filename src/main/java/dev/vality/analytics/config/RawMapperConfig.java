package dev.vality.analytics.config;

import dev.vality.analytics.converter.RawToNamingDistributionConverter;
import dev.vality.analytics.converter.RawToNumModelConverter;
import dev.vality.analytics.converter.RawToShopAmountModelConverter;
import dev.vality.analytics.dao.mapper.CommonRowsMapper;
import dev.vality.analytics.dao.model.NamingDistribution;
import dev.vality.analytics.dao.model.NumberModel;
import dev.vality.analytics.dao.model.ShopAmountModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RawMapperConfig {

    @Bean
    public CommonRowsMapper<NumberModel> costCommonRowsMapper(RawToNumModelConverter rawToNumModelConverter) {
        return new CommonRowsMapper<>(rawToNumModelConverter);
    }

    @Bean
    public CommonRowsMapper<NamingDistribution> namingDistributionCommonRowsMapper(
            RawToNamingDistributionConverter rawToNamingDistributionConverter) {
        return new CommonRowsMapper<>(rawToNamingDistributionConverter);
    }

    @Bean
    public CommonRowsMapper<ShopAmountModel> shopAmountModelCommonRowsMapper(
            RawToShopAmountModelConverter rawToShopAmountModelConverter) {
        return new CommonRowsMapper<>(rawToShopAmountModelConverter);
    }

}
