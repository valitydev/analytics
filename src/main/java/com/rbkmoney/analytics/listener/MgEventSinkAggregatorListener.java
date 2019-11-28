package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.analytics.dao.repository.MgEventSinkRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MgEventSinkAggregatorListener {

    private final MgEventSinkRepository mgEventSinkRepository;

    @KafkaListener(topics = "${kafka.topic.event.sink.aggregated}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<MgEventSinkRow> batch) {
        log.info("MgEventSinkAggregatorListener listen batch.size: {}", batch.size());
        mgEventSinkRepository.insertBatch(batch);
    }
}
