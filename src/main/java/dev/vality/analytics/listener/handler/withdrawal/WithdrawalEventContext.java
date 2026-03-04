package dev.vality.analytics.listener.handler.withdrawal;

import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

@Value
@Builder
public class WithdrawalEventContext {

    MachineEvent machineEvent;
    TimestampedChange timestampedChange;
    LocalDateTime eventTime;
    String withdrawalId;
    WithdrawalStateSnapshot currentState;

}
