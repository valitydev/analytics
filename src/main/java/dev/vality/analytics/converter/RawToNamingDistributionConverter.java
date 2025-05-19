package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.NamingDistribution;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Component
public class RawToNamingDistributionConverter implements Converter<Map, NamingDistribution> {

    public static final String PERCENT = "percent";
    public static final String NAME = "naming_result";

    @Override
    public NamingDistribution convert(Map row) {
        NamingDistribution namingDistribution = new NamingDistribution();
        double percent = BigDecimal.valueOf((Double) row.get(PERCENT))
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
        namingDistribution.setPercent(percent);
        namingDistribution.setName((String) row.get(NAME));
        return namingDistribution;
    }
}
