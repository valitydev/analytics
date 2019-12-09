package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.damsel.analytics.GroupedCurrencyOffsetCount;
import com.rbkmoney.damsel.analytics.GroupedStatusOffsetCount;
import com.rbkmoney.damsel.analytics.OffsetCount;
import com.rbkmoney.damsel.analytics.SplitCountResponse;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GroupedCurCountToResponseConverter {

    public SplitCountResponse convert(List<SplitNumberModel> splitAmount) {
//        Map<String, List<SplitNumberModel>> collect = splitAmount.stream()
//                .collect(Collectors.groupingBy(splitNumberModel -> splitNumberModel.getCurrency()));
//
//        List<GroupedCurrencyOffsetCount> groupedCurrencyOffsetAmounts = collect.entrySet()
//                .stream()
//                .map(stringListEntry -> new GroupedCurrencyOffsetCount()
//                        .setCurrency(stringListEntry.getKey())
//                        .setOffsetAmounts(stringListEntry.getValue().stream()
//                                .map(splitNumberModel -> new GroupedStatusOffsetCount()
//                                        .setOffsetCounts())
//                                .collect(Collectors.toList()))
//                )
//                .collect(Collectors.toList());
//
//        return new SplitCountResponse()
//                .setPaymentToolsDestrobutions(groupedCurrencyOffsetAmounts);

        return null;
    }

}
