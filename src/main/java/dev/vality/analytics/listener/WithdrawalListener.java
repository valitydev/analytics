package dev.vality.analytics.listener;

import dev.vality.analytics.listener.handler.withdrawal.WithdrawalEventHandler;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalListener {

    private final BatchRetryErrorListener batchRetryErrorListener;
    private final WithdrawalEventHandler withdrawalEventHandler;

    @KafkaListener(
            autoStartup = "${kafka.listener.withdrawal.enabled}",
            topics = "${kafka.topic.withdrawal.initial}",
            containerFactory = "withdrawalListenerContainerFactory")
    public void listen(
            List<MachineEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) int offsets,
            Acknowledgment ack) {
        log.info("WithdrawalListener listen offsets: {}, partition: {}, batch.size: {}",
                offsets, partition, batch.size());

        if (CollectionUtils.isEmpty(batch)) {
            ack.acknowledge();
            return;
        }

        try {
            withdrawalEventHandler.handle(batch);
            ack.acknowledge();
        } catch (Exception ex) {
            batchRetryErrorListener.retry("withdrawal-events", batch.size(), ack, ex);
        }
    }
}
