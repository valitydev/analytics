package dev.vality.analytics.converter;

import dev.vality.damsel.analytics.NamingDistribution;
import dev.vality.damsel.analytics.PaymentToolDistributionResponse;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class DaoNamingDistributionsToResponseConverter {

    public PaymentToolDistributionResponse convert(
            List<dev.vality.analytics.dao.model.NamingDistribution> namingDistributions) {
        List<NamingDistribution> collect = namingDistributions.stream()
                .map(paymentToolDistribution -> new NamingDistribution()
                        .setName(paymentToolDistribution.getName())
                        .setPercents(paymentToolDistribution.getPercent())
                ).collect(toList());

        return new PaymentToolDistributionResponse()
                .setPaymentToolsDistributions(collect);
    }

}
