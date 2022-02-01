package dev.vality.analytics.listener.handler.party;

import dev.vality.analytics.listener.handler.ChangeHandler;
import dev.vality.damsel.payment_processing.PartyChange;
import dev.vality.damsel.payment_processing.PartyEventData;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyMachineEventHandler {

    private final MachineEventParser<PartyEventData> eventParser;
    private final List<ChangeHandler<PartyChange, MachineEvent>> partyHandlers;
    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) {
                return;
            }
            for (MachineEvent machineEvent : batch) {
                handleEvent(machineEvent);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            ack.nack(throttlingTimeout);
            throw e;
        }
    }

    private void handleEvent(MachineEvent machineEvent) {
        log.debug("Party Machine event: {}", machineEvent);
        PartyEventData eventData = eventParser.parse(machineEvent);
        if (eventData.isSetChanges()) {
            log.debug("Party changes size: {}", eventData.getChanges().size());
            for (PartyChange change : eventData.getChanges()) {
                log.debug("Party change: {}", change);
                partyHandlers.stream()
                        .filter(changeHandler -> changeHandler.accept(change))
                        .forEach(changeHandler -> changeHandler.handleChange(change, machineEvent));
            }
        }
    }

}
