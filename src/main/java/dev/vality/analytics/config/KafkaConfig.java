package dev.vality.analytics.config;

import dev.vality.analytics.serde.MachineEventDeserializer;
import dev.vality.analytics.serde.PayoutEventDeserializer;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.mg.event.sink.service.ConsumerGroupIdService;
import dev.vality.payout.manager.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;

import java.util.Map;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    public static final String ANALYTICS_RATE = "rate";
    private static final String RESULT_ANALYTICS = "result-analytics";
    private static final String PARTY_RESULT_ANALYTICS = "party-result-analytics";
    private final ConsumerGroupIdService consumerGroupIdService;
    private final KafkaProperties kafkaProperties;

    @Value("${kafka.max.poll.records}")
    private String maxPollRecords;
    @Value("${kafka.topic.party.max.poll.records}")
    private String maxPollRecordsPartyListener;
    @Value("${kafka.topic.rate.max.poll.records}")
    private String maxPollRecordsRatesListener;
    @Value("${kafka.consumer.concurrency}")
    private int concurrencyListeners;
    @Value("${kafka.topic.rate.groupId}")
    private String rateGroupId;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> invoiceListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MachineEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS);
        initDefaultListenerProperties(factory, consumerGroup, new MachineEventDeserializer(), maxPollRecords);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Event> payoutListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Event> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS);
        initDefaultListenerProperties(factory, consumerGroup, new PayoutEventDeserializer(), maxPollRecords);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> partyListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MachineEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(PARTY_RESULT_ANALYTICS);
        initDefaultListenerProperties(factory, consumerGroup,
                new MachineEventDeserializer(), maxPollRecordsPartyListener);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> rateContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MachineEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        initDefaultListenerProperties(factory, rateGroupId, new MachineEventDeserializer(),
                maxPollRecordsRatesListener);
        return factory;
    }

    private <T> void initDefaultListenerProperties(ConcurrentKafkaListenerContainerFactory<String, T> factory,
                                                   String consumerGroup,
                                                   Deserializer<T> deserializer, String maxPollRecords) {
        DefaultKafkaConsumerFactory<String, T> consumerFactory = createKafkaConsumerFactory(
                consumerGroup, deserializer, maxPollRecords);
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrencyListeners);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    }

    @NotNull
    private <T> DefaultKafkaConsumerFactory<String, T> createKafkaConsumerFactory(String consumerGroup,
                                                                                  Deserializer<T> deserializer,
                                                                                  String maxPollRecords) {
        final Map<String, Object> props = createDefaultConsumerProperties(consumerGroup);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer);
    }

    private Map<String, Object> createDefaultConsumerProperties(String value) {
        final Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, value);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.name().toLowerCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }
}
