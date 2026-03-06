package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.Withdrawal;
import org.springframework.stereotype.Component;

@Component
public class WithdrawalCreatedMapper extends AbstractWithdrawalMapper {

    @Override
    public WithdrawalMappingResult map(TimestampedChange change, WithdrawalEventContext context) {
        Withdrawal withdrawal = change.getChange().getCreated().getWithdrawal();
        WithdrawalStateSnapshot currentState = context.getCurrentState();
        WithdrawalStateSnapshot.WithdrawalStateSnapshotBuilder builder = currentState != null
                ? currentState.toBuilder()
                : WithdrawalStateSnapshot.builder().withdrawalId(context.getWithdrawalId());

        builder.withdrawalId(context.getWithdrawalId())
                .partyId(withdrawal.getPartyId())
                .walletId(withdrawal.getWalletId())
                .destinationId(withdrawal.getDestinationId())
                .currency(withdrawal.getBody() != null && withdrawal.getBody().getCurrency() != null
                        ? withdrawal.getBody().getCurrency().getSymbolicCode()
                        : null)
                .requestedAmount(withdrawal.getBody() != null ? withdrawal.getBody().getAmount() : null)
                .withdrawalCreatedAt(parseTime(withdrawal.getCreatedAt()))
                .lastSequenceId(context.getMachineEvent().getEventId())
                .updatedAt(context.getEventTime());

        if (withdrawal.isSetRoute()) {
            builder.providerId(extractProviderId(withdrawal.getRoute()))
                    .terminal(extractTerminal(withdrawal.getRoute()));
        }

        return WithdrawalMappingResult.builder()
                .stateSnapshot(builder.build())
                .build();
    }

    @Override
    public EventType getChangeType() {
        return EventType.WITHDRAWAL_CREATED;
    }
}
