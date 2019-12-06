package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.CountModel;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class RawToCountModelConverter implements Converter<Map, CountModel> {

    public static final String COUNT = "count";
    public static final String CURRENCY = "currency";

    @Override
    public CountModel convert(Map row) {
        CountModel countModel = new CountModel();
        countModel.setCount(((Long) row.get(COUNT)));
        countModel.setCurrency((String) row.get(CURRENCY));
        return countModel;
    }
}
