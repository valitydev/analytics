package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.computer.WithdrawalCashFlowComputer;
import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.fistful.cashflow.FinalCashFlow;
import dev.vality.fistful.withdrawal.TimestampedChange;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class WithdrawalTransferMapper extends AbstractWithdrawalMapper {

    private final WithdrawalCashFlowComputer withdrawalCashFlowComputer;

    @Override
    public boolean accept(TimestampedChange change) {
        return getChangeType().getFilter().match(change)
                && change.getChange().getTransfer().isSetPayload()
                && change.getChange().getTransfer().getPayload().isSetCreated();
    }

    @Override
    public WithdrawalMappingResult map(TimestampedChange change, WithdrawalEventContext context) {
        WithdrawalStateSnapshot currentState = context.getCurrentState();
        if (currentState == null) {
            return null;
        }

        dev.vality.fistful.transfer.Change transferChange = change.getChange().getTransfer().getPayload();
        FinalCashFlow cashFlow = transferChange.getCreated().getTransfer().getCashflow();
        CashFlowResult cashFlowResult = withdrawalCashFlowComputer.compute(
                cashFlow != null ? cashFlow.getPostings() : null);

        return WithdrawalMappingResult.builder()
                .stateSnapshot(currentState.toBuilder()
                        .amount(cashFlowResult.getAmount())
                        .systemFee(cashFlowResult.getSystemFee())
                        .providerFee(cashFlowResult.getProviderFee())
                        .externalFee(cashFlowResult.getExternalFee())
                        .lastSequenceId(context.getMachineEvent().getEventId())
                        .updatedAt(context.getEventTime())
                        .build())
                .build();
    }

    @Override
    public EventType getChangeType() {
        return EventType.WITHDRAWAL_TRANSFER_CHANGED;
    }
}
