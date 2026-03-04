package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.analytics.listener.mapper.Mapper;
import dev.vality.fistful.withdrawal.TimestampedChange;

public interface WithdrawalMapper extends Mapper<TimestampedChange, WithdrawalEventContext, WithdrawalMappingResult> {
}
