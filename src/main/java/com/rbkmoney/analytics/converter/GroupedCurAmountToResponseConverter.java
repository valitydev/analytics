package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.SplitNumberModel;
import com.rbkmoney.damsel.analytics.GroupedCurrencyOffsetAmount;
import com.rbkmoney.damsel.analytics.OffsetAmount;
import com.rbkmoney.damsel.analytics.SplitAmountResponse;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GroupedCurAmountToResponseConverter {

    public SplitAmountResponse convert(List<SplitNumberModel> splitAmount) {
        Map<String, List<SplitNumberModel>> collect = splitAmount.stream()
                .collect(Collectors.groupingBy(splitNumberModel -> splitNumberModel.getCurrency()));

        List<GroupedCurrencyOffsetAmount> groupedCurrencyOffsetAmounts = collect.entrySet()
                .stream()
                .map(stringListEntry -> new GroupedCurrencyOffsetAmount()
                        .setCurrency(stringListEntry.getKey())
                        .setOffsetAmounts(stringListEntry.getValue().stream()
                                .map(splitNumberModel -> new OffsetAmount()
                                        .setOffset(splitNumberModel.getOffset())
                                        .setAmount(splitNumberModel.getNumber()))
                                .collect(Collectors.toList()))
                )
                .collect(Collectors.toList());

        return new SplitAmountResponse().setGroupedCurrencyAmounts(groupedCurrencyOffsetAmounts);
    }

}
