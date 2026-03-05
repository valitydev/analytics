package dev.vality.analytics.dao.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class WithdrawalStateSnapshot {

    private String withdrawalId;
    private String partyId;
    private String walletId;
    private String destinationId;
    private String currency;
    private Long requestedAmount;
    private Long amount;
    private Long systemFee;
    private Long providerFee;
    private LocalDateTime withdrawalCreatedAt;
    private String providerId;
    private String terminal;
    private long lastSequenceId;
    private LocalDateTime updatedAt;

}
