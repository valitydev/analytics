package dev.vality.analytics.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CashFlowResult {

    public static final CashFlowResult EMPTY = CashFlowResult.builder()
            .amount(0L)
            .guaranteeDeposit(0L)
            .systemFee(0L)
            .providerFee(0L)
            .externalFee(0L)
            .build();

    private long amount;
    private long guaranteeDeposit;
    private long systemFee;
    private long providerFee;
    private long externalFee;
}
