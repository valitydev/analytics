package dev.vality.analytics.serde;

import dev.vality.exrates.events.CurrencyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;

@Slf4j
public class CurrencyEventKafkaDeserializer implements Deserializer<CurrencyEvent> {

    ThreadLocal<TDeserializer> thriftDeserializerThreadLocal =
            ThreadLocal.withInitial(() -> {
                try {
                    return new TDeserializer(new TBinaryProtocol.Factory());
                } catch (TTransportException e) {
                    throw new RuntimeException();
                }
            });

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CurrencyEvent deserialize(String topic, byte[] data) {
        log.debug("Message, topic: {}, byteLength: {}", topic, data.length);
        CurrencyEvent currencyEvent = new CurrencyEvent();

        try {
            thriftDeserializerThreadLocal.get().deserialize(currencyEvent, data);
        } catch (Exception e) {
            log.error("Error when deserialize CurrencyEvent data: {} ", data, e);
        }

        return currencyEvent;
    }

    @Override
    public void close() {
    }
}