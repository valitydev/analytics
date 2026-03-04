package dev.vality.analytics.listener.handler.withdrawal;

import dev.vality.analytics.dao.model.WithdrawalRow;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseWithdrawalRepository;
import dev.vality.analytics.dao.repository.postgres.PostgresWithdrawalStateRepository;
import dev.vality.analytics.listener.mapper.withdrawal.WithdrawalMapper;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalEventHandler {

    private final MachineEventParser<TimestampedChange> withdrawalTimestampedChangeMachineEventParser;
    private final PostgresWithdrawalStateRepository postgresWithdrawalStateRepository;
    private final ClickHouseWithdrawalRepository clickHouseWithdrawalRepository;
    private final List<WithdrawalMapper> withdrawalMappers;

    public void handle(List<MachineEvent> batch) {
        if (CollectionUtils.isEmpty(batch)) {
            return;
        }

        Map<String, WithdrawalStateSnapshot> stateCache = new HashMap<>();
        List<WithdrawalRow> withdrawalRows = new ArrayList<>();

        for (MachineEvent machineEvent : batch) {
            WithdrawalEventContext context = prepareContext(machineEvent, stateCache);
            if (context == null) {
                continue;
            }

            WithdrawalMappingResult result = map(context);
            if (result == null) {
                continue;
            }

            apply(result, stateCache, withdrawalRows);
        }

        clickHouseWithdrawalRepository.insertBatch(withdrawalRows);
    }

    private WithdrawalEventContext prepareContext(
            MachineEvent machineEvent,
            Map<String, WithdrawalStateSnapshot> stateCache) {
        TimestampedChange timestampedChange = parse(machineEvent);
        if (timestampedChange == null || !timestampedChange.isSetChange()) {
            return null;
        }

        LocalDateTime eventTime = parseTime(timestampedChange.getOccuredAt());
        if (eventTime == null) {
            log.warn("Skipping withdrawal event with invalid occured_at, sourceId={}, eventId={}",
                    machineEvent.getSourceId(), machineEvent.getEventId());
            return null;
        }

        String withdrawalId = resolveWithdrawalId(machineEvent);
        if (withdrawalId == null) {
            log.warn("Skipping withdrawal event without withdrawal id, eventId={}", machineEvent.getEventId());
            return null;
        }

        WithdrawalStateSnapshot currentState = getState(withdrawalId, stateCache);
        if (currentState != null && machineEvent.getEventId() <= currentState.getLastSequenceId()) {
            log.debug("Skipping stale withdrawal event, withdrawalId={}, eventId={}, lastSequenceId={}",
                    withdrawalId, machineEvent.getEventId(), currentState.getLastSequenceId());
            return null;
        }

        return WithdrawalEventContext.builder()
                .machineEvent(machineEvent)
                .timestampedChange(timestampedChange)
                .eventTime(eventTime)
                .withdrawalId(withdrawalId)
                .currentState(currentState)
                .build();
    }

    private WithdrawalMappingResult map(WithdrawalEventContext context) {
        for (WithdrawalMapper mapper : withdrawalMappers) {
            if (mapper.accept(context.getTimestampedChange())) {
                WithdrawalMappingResult result = mapper.map(context.getTimestampedChange(), context);
                if (result == null) {
                    if (context.getCurrentState() == null) {
                        log.warn("Skipping {} change without reducer state, withdrawalId={}, eventId={}",
                                mapper.getChangeType(),
                                context.getWithdrawalId(),
                                context.getMachineEvent().getEventId());
                    } else {
                        log.debug("Skipping {} change because mapper produced no update, withdrawalId={}, eventId={}",
                                mapper.getChangeType(),
                                context.getWithdrawalId(),
                                context.getMachineEvent().getEventId());
                    }
                }
                return result;
            }
        }

        log.debug("No withdrawal mapper matched, withdrawalId={}, eventId={}",
                context.getWithdrawalId(), context.getMachineEvent().getEventId());
        return null;
    }

    private void apply(
            WithdrawalMappingResult result,
            Map<String, WithdrawalStateSnapshot> stateCache,
            List<WithdrawalRow> withdrawalRows) {
        if (result.getStateSnapshot() != null) {
            upsert(result.getStateSnapshot(), stateCache);
        }

        if (result.getWithdrawalRow() != null) {
            withdrawalRows.add(result.getWithdrawalRow());
        }
    }

    private void upsert(WithdrawalStateSnapshot snapshot, Map<String, WithdrawalStateSnapshot> stateCache) {
        postgresWithdrawalStateRepository.upsert(snapshot);
        stateCache.put(snapshot.getWithdrawalId(), snapshot);
    }

    private WithdrawalStateSnapshot getState(String withdrawalId, Map<String, WithdrawalStateSnapshot> stateCache) {
        WithdrawalStateSnapshot cached = stateCache.get(withdrawalId);
        if (cached != null) {
            return cached;
        }

        Optional<WithdrawalStateSnapshot> stored = postgresWithdrawalStateRepository.findByWithdrawalId(withdrawalId);
        stored.ifPresent(snapshot -> stateCache.put(withdrawalId, snapshot));
        return stored.orElse(null);
    }

    private TimestampedChange parse(MachineEvent machineEvent) {
        try {
            return withdrawalTimestampedChangeMachineEventParser.parse(machineEvent);
        } catch (Exception e) {
            log.warn("Failed to parse withdrawal event, sourceId={}, eventId={}",
                    machineEvent.getSourceId(), machineEvent.getEventId(), e);
            return null;
        }
    }

    private String resolveWithdrawalId(MachineEvent machineEvent) {
        if (machineEvent.isSetSourceId()) {
            return machineEvent.getSourceId();
        }
        return null;
    }

    private LocalDateTime parseTime(String timestamp) {
        if (timestamp == null) {
            return null;
        }

        try {
            return TypeUtil.stringToLocalDateTime(timestamp);
        } catch (Exception e) {
            log.warn("Failed to parse withdrawal timestamp '{}'", timestamp, e);
            return null;
        }
    }
}
