package com.rbkmoney.analytics.dao.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class CommonRowsMapper<T> implements RowsMapper<T> {

    private final Converter<Map, T> converter;

    @Override
    public List<T> map(List<Map<String, Object>> rows) {
        ArrayList<T> resultList = new ArrayList<>();
        for (Map row : rows) {
            T convert = converter.convert(row);
            resultList.add(convert);
        }
        return resultList;
    }

}
