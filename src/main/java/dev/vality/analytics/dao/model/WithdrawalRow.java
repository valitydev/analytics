package dev.vality.analytics.dao.model;

import dev.vality.analytics.constant.WithdrawalStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WithdrawalRow {

    private LocalDateTime eventTime;
    private String partyId;
    private String withdrawalId;
    private long sequenceId;
    private LocalDateTime withdrawalTime;
    private String walletId;
    private String destinationId;
    private String providerId;
    private String terminal;
    private long amount;
    private long systemFee;
    private long providerFee;
    private String currency;
    private WithdrawalStatus status;

}
