package dev.vality.analytics.listener;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import dev.vality.analytics.listener.handler.dominant.common.DominantHandler;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class DominantListener {

    private final List<DominantHandler> dominantHandlers;
    private final MachineEventParser<HistoricalCommit> eventParser;

    @KafkaListener(autoStartup = "${kafka.listener.dominant.enabled}",
            topics = "${kafka.topic.dominant.initial}",
            containerFactory = "dominantListenerContainerFactory")
    public void listen(List<MachineEvent> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) {
        log.info("DominantListener listen offsets: {} partition: {} batch.size: {}", offsets, partition, batch.size());
        batch.forEach(this::handle);
        log.info("DominantListener batch has been commited, batch.size={}", batch.size());
        ack.acknowledge();
    }

    public void handle(MachineEvent machineEvent) {
        var commit = eventParser.parse(machineEvent);
        commit.getOps().forEach(op ->
                dominantHandlers.stream()
                        .filter(handler -> handler.isHandle(op))
                        .forEach(handler ->
                                handler.handle(op, commit)
                        )
        );
    }

}
