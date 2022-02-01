package dev.vality.analytics.converter;

import dev.vality.analytics.dao.model.SplitStatusNumberModel;
import dev.vality.analytics.exception.PaymentInfoRequestException;
import dev.vality.damsel.analytics.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Service
public class GroupedCurCountToResponseConverter {

    private static final String CAPTURED_STATUS = "captured";
    private static final String FAILED_STATUS = "failed";
    private static final String CANCELLED_STATUS = "cancelled";

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

    private GroupedCurrencyOffsetCount initGroupedCurrencyOffsetCount(Map.Entry<String,
            Map<String, List<SplitStatusNumberModel>>> entry) {
        return new GroupedCurrencyOffsetCount()
                .setCurrency(entry.getKey())
                .setOffsetAmounts(entry.getValue().entrySet()
                        .stream()
                        .map(this::initGroupedStatusOffsetCount)
                        .collect(toList())
                );
    }

    private GroupedStatusOffsetCount initGroupedStatusOffsetCount(
            Map.Entry<String,
                    List<SplitStatusNumberModel>> entry) {
        return new GroupedStatusOffsetCount()
                .setStatus(mapStatus(entry))
                .setOffsetCounts(entry.getValue().stream()
                        .map(splitStatusNumberModel -> new OffsetCount()
                                .setCount(splitStatusNumberModel.getNumber())
                                .setOffset(splitStatusNumberModel.getOffset())
                        ).collect(toList())
                );
    }

    private PaymentStatus mapStatus(Map.Entry<String, List<SplitStatusNumberModel>> entry) {
        return switch (entry.getKey()) {
            case CAPTURED_STATUS -> PaymentStatus.CAPTURED;
            case FAILED_STATUS -> PaymentStatus.FAILED;
            case CANCELLED_STATUS -> PaymentStatus.CANCELLED;
            default -> throw new PaymentInfoRequestException();
        };
    }

}
