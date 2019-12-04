package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.mg.event.sink.EventSinkAggregationStreamFactoryImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    public static final long CLOSE_STREAM_TIMEOUT_SECONDS = 10L;

    @Value("${kafka.stream.event.sink.enable}")
    private boolean enableEventSinkStream;

    private final Properties eventSinkPaymentStreamProperties;
    private final Properties eventSinkRefundStreamProperties;
    private final EventSinkAggregationStreamFactoryImpl<String, MgPaymentSinkRow, MgPaymentSinkRow> eventSinkAggregationStreamFactory;
    private final EventSinkAggregationStreamFactoryImpl<String, MgRefundRow, MgRefundRow> eventSinkRefundAggregationStreamFactory;

    private List<KafkaStreams> eventSinkStreams = new ArrayList<>();

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        long startPreloadTime = System.currentTimeMillis();
        startEventStream(startPreloadTime);
    }

    private void startEventStream(long startPreloadTime) {
        if (enableEventSinkStream) {
//            KafkaStreams eventSinkStream = eventSinkAggregationStreamFactory.create(eventSinkPaymentStreamProperties);
//            eventSinkStreams.add(eventSinkStream);
            KafkaStreams eventSinkStreamRefund = eventSinkRefundAggregationStreamFactory.create(eventSinkRefundStreamProperties);
            eventSinkStreams.add(eventSinkStreamRefund);
//            log.info("StartupListener start stream preloadTime: {} ms eventSinkStream: {} eventSinkStreamRefund: {}", System.currentTimeMillis() - startPreloadTime,
//                    eventSinkStream.allMetadata(), eventSinkStreamRefund.allMetadata());
        }
    }

    public void stop() {
        if (eventSinkStreams != null && !eventSinkStreams.isEmpty()) {
            eventSinkStreams.forEach(kafkaStreams -> kafkaStreams.close(Duration.ofSeconds(CLOSE_STREAM_TIMEOUT_SECONDS)));

        }
    }

}