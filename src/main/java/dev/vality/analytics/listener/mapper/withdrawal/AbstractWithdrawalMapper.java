package dev.vality.analytics.listener.mapper.withdrawal;

import dev.vality.analytics.constant.WithdrawalStatus;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventContext;
import dev.vality.analytics.listener.handler.withdrawal.WithdrawalMappingResult;
import dev.vality.analytics.listener.mapper.AbstractMapper;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;

import java.time.LocalDateTime;

public abstract class AbstractWithdrawalMapper
        extends AbstractMapper<TimestampedChange, WithdrawalEventContext, WithdrawalMappingResult>
        implements WithdrawalMapper {

    protected LocalDateTime parseTime(String timestamp) {
        if (timestamp == null) {
            return null;
        }

        try {
            return TypeUtil.stringToLocalDateTime(timestamp);
        } catch (Exception e) {
            return null;
        }
    }

    protected String extractProviderId(dev.vality.fistful.withdrawal.Route route) {
        if (route == null) {
            return null;
        }

        if (route.isSetProviderId()) {
            return String.valueOf(route.getProviderId());
        }

        if (route.isSetProviderIdLegacy()) {
            return route.getProviderIdLegacy();
        }

        return null;
    }

    protected String extractTerminal(dev.vality.fistful.withdrawal.Route route) {
        if (route == null) {
            return null;
        }

        if (route.isSetTerminalId()) {
            return String.valueOf(route.getTerminalId());
        }

        if (route.isSetTerminalIdLegacy()) {
            return route.getTerminalIdLegacy();
        }

        return null;
    }

    protected WithdrawalStatus mapStatus(dev.vality.fistful.withdrawal.status.Status status) {
        if (status == null) {
            return null;
        }

        if (status.isSetPending()) {
            return WithdrawalStatus.pending;
        }

        if (status.isSetSucceeded()) {
            return WithdrawalStatus.succeeded;
        }

        if (status.isSetFailed()) {
            return WithdrawalStatus.failed;
        }

        return null;
    }
}
