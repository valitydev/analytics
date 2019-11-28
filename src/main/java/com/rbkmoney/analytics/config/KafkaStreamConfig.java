package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.analytics.serde.MgEventSinkRowDeserializer;
import com.rbkmoney.analytics.serde.MgEventSinkRowSerde;
import com.rbkmoney.analytics.stream.aggregate.MgEventAggregator;
import com.rbkmoney.mg.event.sink.EventSinkAggregationStreamFactoryImpl;
import com.rbkmoney.mg.event.sink.MgEventSinkRowMapper;
import com.rbkmoney.mg.event.sink.converter.SinkEventToEventPayloadConverter;
import com.rbkmoney.mg.event.sink.handler.MgEventSinkHandlerExecutor;
import com.rbkmoney.mg.event.sink.handler.flow.EventHandler;
import com.rbkmoney.mg.event.sink.serde.SinkEventSerde;
import com.rbkmoney.mg.event.sink.service.ConsumerGroupIdService;
import com.rbkmoney.mg.event.sink.utils.SslKafkaUtils;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class KafkaStreamConfig {

    private static final String EVENT_SINK_CLIENT_ANALYTICS = "event-sink-client-analytics";
    public static final String RESULT_ANALYTICS = "result-analytics";
    private static final String EARLIEST = "earliest";

    @Value("${kafka.state.cache.size:10}")
    private int cacheSizeStateStoreMb;

    @Value("${kafka.max.poll.records}")
    private String maxPollRecords;

    @Value("${kafka.state.dir}")
    private String stateDir;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.ssl.server-password}")
    private String serverStorePassword;

    @Value("${kafka.ssl.server-keystore-location}")
    private String serverStoreCertPath;

    @Value("${kafka.ssl.keystore-password}")
    private String keyStorePassword;

    @Value("${kafka.ssl.key-password}")
    private String keyPassword;

    @Value("${kafka.ssl.keystore-location}")
    private String clientStoreCertPath;

    @Value("${kafka.ssl.enable}")
    private boolean kafkaSslEnable;

    @Value("${kafka.topic.event.sink.initial}")
    private String initialEventSink;

    @Value("${kafka.topic.event.sink.aggregated}")
    private String aggregatedSinkTopic;

    private final ConsumerGroupIdService consumerGroupIdService;
    public final List<EventHandler<MgEventSinkRow>> eventHandlers;

    @Bean
    public Properties eventSinkStreamProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupIdService.generateGroupId("mg-event-sink"));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, EVENT_SINK_CLIENT_ANALYTICS);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MgEventSinkRowSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeStateStoreMb * 1024 * 1024L);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.putAll(SslKafkaUtils.sslConfigure(kafkaSslEnable, serverStoreCertPath, serverStorePassword,
                clientStoreCertPath, keyStorePassword, keyPassword));
        return props;
    }

    @Bean
    public EventSinkAggregationStreamFactoryImpl<String, MgEventSinkRow, MgEventSinkRow> eventSinkAggregationStreamFactory(
            MgEventAggregator mgEventAggregator,
            MgEventSinkRowMapper<MgEventSinkRow> mgEventSinkRowMgEventSinkRowMapper) {
        return new EventSinkAggregationStreamFactoryImpl<>(
                initialEventSink,
                aggregatedSinkTopic,
                new SinkEventSerde(),
                Serdes.String(),
                new MgEventSinkRowSerde(),
                MgEventSinkRow::new,
                mgEventAggregator,
                mgEventSinkRowMgEventSinkRowMapper,
                mgEventSinkRow -> mgEventSinkRow.getStatus() != null
        );
    }

    @Bean
    public MgEventSinkHandlerExecutor<MgEventSinkRow> mgEventSinkHandler(
            SinkEventToEventPayloadConverter sinkEventToEventPayloadConverter) {
        return new MgEventSinkHandlerExecutor<>(sinkEventToEventPayloadConverter, eventHandlers);
    }

    @Bean
    public MgEventSinkRowMapper<MgEventSinkRow> mgEventSinkRowMgEventSinkRowMapper(MgEventSinkHandlerExecutor<MgEventSinkRow> mgEventSinkHandler) {
        return new MgEventSinkRowMapper<>(mgEventSinkHandler);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MgEventSinkRow> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MgEventSinkRow> factory = new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS);
        final Map<String, Object> props = createDefaultProperties(consumerGroup);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        DefaultKafkaConsumerFactory<String, MgEventSinkRow> consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new MgEventSinkRowDeserializer());
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        return factory;
    }

    @NotNull
    private Map<String, Object> createDefaultProperties(String value) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, value);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.putAll(SslKafkaUtils.sslConfigure(kafkaSslEnable, serverStoreCertPath, serverStorePassword,
                clientStoreCertPath, keyStorePassword, keyPassword));
        return props;
    }
}
