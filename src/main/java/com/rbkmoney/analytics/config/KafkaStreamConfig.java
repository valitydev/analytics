package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.KafkaSslProperties;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.serde.MgPaymentRowDeserializer;
import com.rbkmoney.analytics.serde.MgPaymentRowSerde;
import com.rbkmoney.analytics.serde.MgRefundRowDeserializer;
import com.rbkmoney.analytics.serde.MgRefundRowSerde;
import com.rbkmoney.analytics.stream.aggregate.MgPaymentAggregator;
import com.rbkmoney.analytics.stream.aggregate.MgRefundAggregator;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.EventSinkAggregationStreamFactoryImpl;
import com.rbkmoney.mg.event.sink.EventStreamFactory;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KeyValueMapper;
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

    public static final String EVENT_SINK_CLIENT_ANALYTICS = "event-sink-client-analytics";
    public static final String EVENT_SINK_CLIENT_ANALYTICS_REFUND = "event-sink-client-analytics-refund";

    private static final String RESULT_ANALYTICS = "result-analytics";
    private static final String RESULT_ANALYTICS_REFUND = "result-analytics-refund";
    private static final String EARLIEST = "earliest";
    public static final String MG_EVENT_SINK_PAYMENT = "mg-event-sink-payment";
    public static final String MG_EVENT_SINK_PAYMENT_REFUND = "mg-event-sink-paymentrefund";

    @Value("${kafka.state.cache.size:10}")
    private int cacheSizeStateStoreMb;

    @Value("${kafka.max.poll.records}")
    private String maxPollRecords;

    @Value("${kafka.state.dir}")
    private String stateDir;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.event.sink.initial}")
    private String initialEventSink;

    @Value("${kafka.topic.event.sink.aggregated}")
    private String aggregatedSinkTopic;


    @Value("${kafka.topic.event.sink.aggregatedRefund}")
    private String aggregatedSinkTopicRefund;

    private final ConsumerGroupIdService consumerGroupIdService;
    private final List<EventHandler<MgPaymentSinkRow>> eventHandlers;
    private final List<EventHandler<MgRefundRow>> eventRefundHandlers;
    private final KafkaSslProperties kafkaSslProperties;

    @Bean
    public Properties eventSinkPaymentStreamProperties() {
        Properties props = createDefaultProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupIdService.generateGroupId(MG_EVENT_SINK_PAYMENT));
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MgPaymentRowSerde.class);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, EVENT_SINK_CLIENT_ANALYTICS);
        return props;
    }

    @Bean
    public Properties eventSinkRefundStreamProperties() {
        Properties props = createDefaultProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupIdService.generateGroupId(MG_EVENT_SINK_PAYMENT_REFUND));
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MgRefundRowSerde.class);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, EVENT_SINK_CLIENT_ANALYTICS_REFUND);
        return props;
    }

    private Properties createDefaultProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeStateStoreMb * 1024 * 1024L);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.putAll(createSslConfig());
        return props;
    }

    private Map<String, Object> createSslConfig() {
        return SslKafkaUtils.sslConfigure(
                kafkaSslProperties.isKafkaSslEnable(),
                kafkaSslProperties.getServerStoreCertPath(),
                kafkaSslProperties.getServerStorePassword(),
                kafkaSslProperties.getClientStoreCertPath(),
                kafkaSslProperties.getKeyStorePassword(),
                kafkaSslProperties.getKeyPassword());
    }

    @Bean
    public EventSinkAggregationStreamFactoryImpl<String, MgPaymentSinkRow, MgPaymentSinkRow> eventSinkAggregationStreamFactory(
            MgPaymentAggregator mgPaymentAggregator,
            MgEventSinkRowMapper<MgPaymentSinkRow> mgEventSinkRowMgEventSinkRowMapper) {
        return new EventSinkAggregationStreamFactoryImpl<>(
                initialEventSink,
                aggregatedSinkTopic,
                new SinkEventSerde(),
                Serdes.String(),
                new MgPaymentRowSerde(),
                MgPaymentSinkRow::new,
                mgPaymentAggregator,
                mgEventSinkRowMgEventSinkRowMapper,
                mgEventSinkRow -> mgEventSinkRow.getStatus() != null,
                (key, value) -> value.getInvoiceId() + "_" + value.getPaymentId());
    }

//
//    @Bean
//    public EventStreamFactory eventSinkAggregationStreamFactory(
//            MgPaymentAggregator mgPaymentAggregator,
//            KeyValueMapper<String, SinkEvent, KeyValue<String, List<MgPaymentSinkRow>>> mgEventSinkRowMgEventSinkRowMapper,
//            MgRefundAggregator mgRefundAggregator,
//            KeyValueMapper<String, SinkEvent, KeyValue<String, List<MgRefundRow>>> mgRefundRowRowMgEventSinkRowMapper) {
//        return new JoinedEventSinkAggregationStreamFactoryImpl(initialEventSink, aggregatedSinkTopic, aggregatedSinkTopicRefund,
//                mgPaymentAggregator, mgRefundAggregator, mgEventSinkRowMgEventSinkRowMapper, mgRefundRowRowMgEventSinkRowMapper);
//    }

    @Bean
    public EventSinkAggregationStreamFactoryImpl<String, MgRefundRow, MgRefundRow> eventSinkRefundAggregationStreamFactory(
            MgRefundAggregator mgRefundAggregator,
            MgEventSinkRowMapper<MgRefundRow> mgRefundRowRowMgEventSinkRowMapper) {
        return new EventSinkAggregationStreamFactoryImpl<>(
                initialEventSink,
                aggregatedSinkTopicRefund,
                new SinkEventSerde(),
                Serdes.String(),
                new MgRefundRowSerde(),
                MgRefundRow::new,
                mgRefundAggregator,
                mgRefundRowRowMgEventSinkRowMapper,
                mgRefundRow -> mgRefundRow.getStatus() != null,
                (key, value) -> value.getInvoiceId() + "_" + value.getPaymentId());
    }

    @Bean
    public MgEventSinkHandlerExecutor<MgPaymentSinkRow> mgEventSinkHandler(
            SinkEventToEventPayloadConverter sinkEventToEventPayloadConverter) {
        return new MgEventSinkHandlerExecutor<>(sinkEventToEventPayloadConverter, eventHandlers);
    }

    @Bean
    public MgEventSinkRowMapper<MgPaymentSinkRow> mgRefundRowRowMgEventSinkRowMapper(MgEventSinkHandlerExecutor<MgPaymentSinkRow> mgEventSinkHandler) {
        return new MgEventSinkRowMapper<>(mgEventSinkHandler);
    }

    @Bean
    public MgEventSinkRowMapper<MgRefundRow> mgEventSinkRowMgEventSinkRowMapper(MgEventSinkHandlerExecutor<MgRefundRow> mgRefundRowHandler) {
        return new MgEventSinkRowMapper<>(mgRefundRowHandler);
    }

    @Bean
    public MgEventSinkHandlerExecutor<MgRefundRow> mgRefundRowHandler(
            SinkEventToEventPayloadConverter sinkEventToEventPayloadConverter) {
        return new MgEventSinkHandlerExecutor<>(sinkEventToEventPayloadConverter, eventRefundHandlers);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MgPaymentSinkRow> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MgPaymentSinkRow> factory = new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS);
        final Map<String, Object> props = createDefaultProperties(consumerGroup);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        DefaultKafkaConsumerFactory<String, MgPaymentSinkRow> consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new MgPaymentRowDeserializer());
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MgRefundRow> kafkaListenerRefundContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MgRefundRow> factory = new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS_REFUND);
        final Map<String, Object> props = createDefaultProperties(consumerGroup);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        DefaultKafkaConsumerFactory<String, MgRefundRow> consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new MgRefundRowDeserializer());
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
        props.putAll(createSslConfig());
        return props;
    }
}
