package dev.vality.analytics.listener.handler.withdrawal;

import dev.vality.analytics.dao.model.WithdrawalRow;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class WithdrawalMappingResult {

    WithdrawalStateSnapshot stateSnapshot;
    WithdrawalRow withdrawalRow;

}
