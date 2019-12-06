package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.Cost;
import com.rbkmoney.damsel.analytics.AmountResponse;
import com.rbkmoney.damsel.analytics.CurrencyGroupedAmount;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class CostToAmountResponse {

    public AmountResponse convert(List<Cost> costs) {
        List<CurrencyGroupedAmount> collect = costs.stream()
                .map(cost -> new CurrencyGroupedAmount()
                        .setAmount(cost.getAmount())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new AmountResponse()
                .setGroupsAmount(collect);
    }

}
