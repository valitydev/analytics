package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.mg.event.sink.EventSinkAggregationStreamFactoryImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    public static final long CLOSE_STREAM_TIMEOUT_SECONDS = 10L;

    @Value("${kafka.stream.event.sink.enable}")
    private boolean enableEventSinkStream;

    private final Properties eventSinkStreamProperties;
    private final EventSinkAggregationStreamFactoryImpl<String, MgEventSinkRow, MgEventSinkRow> eventSinkAggregationStreamFactory;

    private KafkaStreams eventSinkStream;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        long startPreloadTime = System.currentTimeMillis();
        startEventStream(startPreloadTime);
    }

    private void startEventStream(long startPreloadTime) {
        if (enableEventSinkStream) {
            eventSinkStream = eventSinkAggregationStreamFactory.create(eventSinkStreamProperties);
            log.info("StartupListener start stream preloadTime: {} ms eventSinkStream: {}", System.currentTimeMillis() - startPreloadTime,
                    eventSinkStream.allMetadata());
        }
    }

    public void stop() {
        if (eventSinkStream != null) {
            eventSinkStream.close(Duration.ofSeconds(CLOSE_STREAM_TIMEOUT_SECONDS));
        }
    }

}