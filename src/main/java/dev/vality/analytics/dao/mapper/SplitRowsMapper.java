package dev.vality.analytics.dao.mapper;

import dev.vality.analytics.converter.RawToSplitNumberConverter;
import dev.vality.analytics.dao.model.SplitNumberModel;
import dev.vality.damsel.analytics.SplitUnit;
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
