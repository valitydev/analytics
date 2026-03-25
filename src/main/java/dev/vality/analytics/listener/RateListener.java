package dev.vality.analytics.listener;

import dev.vality.analytics.listener.handler.rate.CurrencyEventHandler;
import dev.vality.exrates.events.CurrencyEvent;
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
public class RateListener {

    private final CurrencyEventHandler currencyEventHandler;

    @KafkaListener(autoStartup = "${kafka.listener.rate.enabled}",
            topics = "${kafka.topic.rate.initial}",
            containerFactory = "rateContainerFactory")
    public void handle(List<CurrencyEvent> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) {
        log.info("Got RateListener listen offsets: {}, partition: {}, batch.size: {}",
                offsets, partition, batch.size());
        currencyEventHandler.handle(batch, ack);
        log.info("Batch RateListener has been committed, size={}", batch.size());
    }

}
