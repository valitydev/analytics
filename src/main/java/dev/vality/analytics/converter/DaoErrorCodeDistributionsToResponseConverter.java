package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.NamingDistribution;
import dev.vality.analytics.utils.SubErrorGenerator;
import dev.vality.damsel.analytics.ErrorDistribution;
import dev.vality.damsel.analytics.SubErrorDistributionsResponse;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class DaoErrorCodeDistributionsToResponseConverter {

    public SubErrorDistributionsResponse convert(
            List<NamingDistribution> namingDistributions) {
        List<ErrorDistribution> collect = namingDistributions.stream()
                .map(paymentToolDistribution -> new ErrorDistribution()
                        .setError(SubErrorGenerator.generateError(paymentToolDistribution.getName()))
                        .setPercents(paymentToolDistribution.getPercent())
                ).collect(toList());

        return new SubErrorDistributionsResponse()
                .setErrorDistributions(collect);
    }

}
