package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.CountModel;
import com.rbkmoney.damsel.analytics.CountResponse;
import com.rbkmoney.damsel.analytics.CurrecyGroupCount;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class CountModelCountResponseConverter {

    public CountResponse convert(List<CountModel> countModels) {
        List<CurrecyGroupCount> collect = countModels.stream()
                .map(cost -> new CurrecyGroupCount()
                        .setCount(cost.getCount())
                        .setCurrency(cost.getCurrency())
                ).collect(toList());

        return new CountResponse()
                .setGroupsCount(collect);
    }

}
