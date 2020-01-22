package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.analytics.dao.utils.SplitUtils;
import com.rbkmoney.damsel.analytics.SplitUnit;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.Map;

@Component
public class RawToSplitNumberConverter {

    public static final String NUM = "num";
    public static final String CURRENCY = "currency";

    public SplitNumberModel convert(Map row, SplitUnit splitUnit) {
        SplitNumberModel cost = new SplitNumberModel();
        if (row.get(NUM) instanceof Double) {
            cost.setNumber((((Double) row.get(NUM)).longValue()));
        }
        if (row.get(NUM) instanceof Long) {
            cost.setNumber((Long) row.get(NUM));
        } else {
            cost.setNumber(((BigInteger) row.get(NUM)).longValue());
        }
        cost.setOffset(SplitUtils.generateOffset(row, splitUnit));
        cost.setCurrency((String) row.get(CURRENCY));
        return cost;
    }

}
