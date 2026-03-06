package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.constant.WithdrawalStatus;
import dev.vality.analytics.dao.model.WithdrawalRow;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.fistful.withdrawal.TimestampedChange;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class WithdrawalStatusMapper extends AbstractWithdrawalMapper {

    @Override
    public WithdrawalMappingResult map(TimestampedChange change, WithdrawalEventContext context) {
        WithdrawalStateSnapshot currentState = context.getCurrentState();
        if (currentState == null) {
            return null;
        }

        WithdrawalStatus status = mapStatus(change.getChange().getStatusChanged().getStatus());
        if (status == null) {
            return null;
        }

        WithdrawalStateSnapshot updatedState = currentState.toBuilder()
                .lastSequenceId(context.getMachineEvent().getEventId())
                .updatedAt(context.getEventTime())
                .build();

        WithdrawalRow row = WithdrawalRow.builder()
                .eventTime(context.getEventTime())
                .partyId(updatedState.getPartyId())
                .withdrawalId(updatedState.getWithdrawalId())
                .sequenceId(context.getMachineEvent().getEventId())
                .withdrawalTime(Optional.ofNullable(updatedState.getWithdrawalCreatedAt())
                        .orElse(context.getEventTime()))
                .walletId(updatedState.getWalletId())
                .destinationId(updatedState.getDestinationId())
                .providerId(updatedState.getProviderId())
                .terminal(updatedState.getTerminal())
                .amount(Optional.ofNullable(updatedState.getAmount())
                        .orElse(Optional.ofNullable(updatedState.getRequestedAmount()).orElse(0L)))
                .systemFee(Optional.ofNullable(updatedState.getSystemFee()).orElse(0L))
                .providerFee(Optional.ofNullable(updatedState.getProviderFee()).orElse(0L))
                .currency(updatedState.getCurrency())
                .status(status)
                .build();

        return WithdrawalMappingResult.builder()
                .stateSnapshot(updatedState)
                .withdrawalRow(row)
                .build();
    }

    @Override
    public EventType getChangeType() {
        return EventType.WITHDRAWAL_STATUS_CHANGED;
    }
}
