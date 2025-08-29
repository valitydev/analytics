package dev.vality.analytics.listener;

import dev.vality.analytics.listener.handler.dominant.common.DominantHandler;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DominantListener {

    private final List<DominantHandler> dominantHandlers;

    @KafkaListener(autoStartup = "${kafka.listener.dominant.enabled}",
            topics = "${kafka.topic.dominant.initial}",
            containerFactory = "dominantListenerContainerFactory")
    public void listen(List<HistoricalCommit> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) {
        log.info("DominantListener listen offsets: {} partition: {} batch.size: {}", offsets, partition, batch.size());
        batch.forEach(commit -> {
            commit.getOps().forEach(op ->
                    dominantHandlers.stream()
                            .filter(handler -> handler.isHandle(op))
                            .forEach(handler ->
                                    handler.handle(op, commit)
                            )
            );
        });
        log.info("DominantListener batch has been commited, batch.size={}", batch.size());
        ack.acknowledge();
    }

}
