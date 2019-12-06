package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.SplitCost;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class RawToSplitCostConverter implements Converter<Map, SplitCost> {

    public static final String AMOUNT = "amount";
    public static final String CURRENCY = "currency";

    @Override
    public SplitCost convert(Map row) {
        SplitCost cost = new SplitCost();
        if (row.get(AMOUNT) instanceof Double) {
            cost.setAmount((((Double) row.get(AMOUNT)).longValue()));
        } else {
            cost.setAmount(((Long) row.get(AMOUNT)));
        }
        cost.setOffset(((Long) row.get("unit")));
        cost.setCurrency((String) row.get(CURRENCY));
        return cost;
    }
}
