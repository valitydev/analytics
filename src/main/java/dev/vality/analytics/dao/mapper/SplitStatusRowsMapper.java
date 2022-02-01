package dev.vality.analytics.dao.mapper;

import dev.vality.analytics.converter.RawToSplitStatusConverter;
import dev.vality.analytics.dao.model.SplitStatusNumberModel;
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
public class SplitStatusRowsMapper {

    private final RawToSplitStatusConverter converter;

    public List<SplitStatusNumberModel> map(List<Map<String, Object>> rows, SplitUnit splitUnit) {
        List<SplitStatusNumberModel> resultList = new ArrayList<>();
        for (Map row : rows) {
            SplitStatusNumberModel convert = converter.convert(row, splitUnit);
            resultList.add(convert);
        }
        log.debug("SplitRowsMapper resultList: {}", resultList);
        return resultList;
    }

}
