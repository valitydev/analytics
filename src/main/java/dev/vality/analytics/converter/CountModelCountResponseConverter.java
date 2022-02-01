package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.NumberModel;
import dev.vality.damsel.analytics.CountResponse;
import dev.vality.damsel.analytics.CurrecyGroupCount;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class CountModelCountResponseConverter {

    public CountResponse convert(List<NumberModel> countModels) {
        List<CurrecyGroupCount> collect = countModels.stream()
                .map(cost -> new CurrecyGroupCount()
                        .setCount(cost.getNumber())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new CountResponse()
                .setGroupsCount(collect);
    }

}
