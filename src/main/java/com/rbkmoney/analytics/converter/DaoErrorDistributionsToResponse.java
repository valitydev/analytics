package com.rbkmoney.analytics.converter;

import com.rbkmoney.damsel.analytics.ErrorDistributionsResponse;
import com.rbkmoney.damsel.analytics.NamingDistribution;
import com.rbkmoney.damsel.analytics.PaymentToolDistributionResponse;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class DaoErrorDistributionsToResponse {

    public ErrorDistributionsResponse convert(List<com.rbkmoney.analytics.dao.model.NamingDistribution> namingDistributions) {
        List<NamingDistribution> collect = namingDistributions.stream()
                .map(paymentToolDistribution -> new NamingDistribution()
                        .setName(paymentToolDistribution.getName())
                        //TODO after proto to double
                        .setPercents(paymentToolDistribution.getPercent().longValue())
                ).collect(toList());

        return new ErrorDistributionsResponse()
                .setErrorDistributions(collect);
    }

}
