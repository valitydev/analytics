package com.rbkmoney.analytics.converter;

import com.rbkmoney.analytics.dao.model.SplitStatusNumberModel;
import com.rbkmoney.damsel.analytics.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Service
public class GroupedCurCountToResponseConverter {

    public SplitCountResponse convert(List<SplitStatusNumberModel> splitAmount) {
        Map<String, Map<String, List<SplitStatusNumberModel>>> maps = splitAmount.stream()
                .collect(groupingBy(SplitStatusNumberModel::getCurrency,
                        groupingBy(SplitStatusNumberModel::getStatus)));

        List<GroupedCurrencyOffsetCount> groupedCurrencyOffsetAmounts = maps.entrySet()
                .stream()
                .map(this::initGroupedCurrencyOffsetCount)
                .collect(toList());

        return new SplitCountResponse()
                .setPaymentToolsDestrobutions(groupedCurrencyOffsetAmounts);
    }

    private GroupedCurrencyOffsetCount initGroupedCurrencyOffsetCount(Map.Entry<String, Map<String, List<SplitStatusNumberModel>>> entry) {
        return new GroupedCurrencyOffsetCount()
                .setCurrency(entry.getKey())
                .setOffsetAmounts(entry.getValue().entrySet()
                        .stream()
                        .map(this::initGroupedStatusOffsetCount)
                        .collect(toList())
                );
    }

    private GroupedStatusOffsetCount initGroupedStatusOffsetCount(Map.Entry<String, List<SplitStatusNumberModel>> entry) {
        return new GroupedStatusOffsetCount()
                .setStatus(PaymentStatus.valueOf(entry.getKey()))
                .setOffsetCounts(entry.getValue().stream()
                        .map(splitStatusNumberModel -> new OffsetCount()
                                .setCount(splitStatusNumberModel.getNumber())
                                .setOffset(splitStatusNumberModel.getOffset())
                        ).collect(toList())
                );
    }

}
