package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.fistful.withdrawal.TimestampedChange;
import org.springframework.stereotype.Component;

@Component
public class WithdrawalRouteMapper extends AbstractWithdrawalMapper {

    @Override
    public WithdrawalMappingResult map(TimestampedChange change, WithdrawalEventContext context) {
        WithdrawalStateSnapshot currentState = context.getCurrentState();
        if (currentState == null) {
            return null;
        }

        dev.vality.fistful.withdrawal.Route route = change.getChange().getRoute().getRoute();
        return WithdrawalMappingResult.builder()
                .stateSnapshot(currentState.toBuilder()
                        .providerId(extractProviderId(route))
                        .terminal(extractTerminal(route))
                        .lastSequenceId(context.getMachineEvent().getEventId())
                        .updatedAt(context.getEventTime())
                        .build())
                .build();
    }

    @Override
    public EventType getChangeType() {
        return EventType.WITHDRAWAL_ROUTE_CHANGED;
    }
}
