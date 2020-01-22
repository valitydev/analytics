package com.rbkmoney.analytics.dao.mapper;

import com.rbkmoney.analytics.converter.RawToSplitNumberConverter;
import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SplitRowsMapper {

    private final RawToSplitNumberConverter converter;

    public List<SplitNumberModel> map(List<Map<String, Object>> rows, SplitUnit splitUnit) {
        ArrayList<SplitNumberModel> resultList = new ArrayList<>();
        for (Map row : rows) {
            SplitNumberModel convert = converter.convert(row, splitUnit);
            resultList.add(convert);
        }
        log.debug("SplitRowsMapper resultList: {}", resultList);
        return resultList;
    }

}
