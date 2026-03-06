package dev.vality.analytics.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WithdrawalCashFlowResult {

    public static final WithdrawalCashFlowResult EMPTY = new WithdrawalCashFlowResult(0L, 0L, 0L);

    private final long amount;
    private final long systemFee;
    private final long providerFee;
}
