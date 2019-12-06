package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.Cost;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class RawToCostConverter implements Converter<Map, Cost> {

    public static final String AMOUNT = "amount";
    public static final String CURRENCY = "currency";

    @Override
    public Cost convert(Map row) {
        Cost cost = new Cost();
        if (row.get(AMOUNT) instanceof Double) {
            cost.setAmount((((Double) row.get(AMOUNT)).longValue()));
        } else {
            cost.setAmount(((Long) row.get(AMOUNT)));
        }
        cost.setCurrency((String) row.get(CURRENCY));
        return cost;
    }
}
