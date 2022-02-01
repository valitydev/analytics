package dev.vality.analytics.converter;

import dev.vality.damsel.payment_processing.EventPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BinaryConverterImpl implements BinaryConverter<EventPayload> {

    ThreadLocal<TDeserializer> thriftDeserializerThreadLocal =
            ThreadLocal.withInitial(() -> {
                try {
                    return new TDeserializer(new TBinaryProtocol.Factory());
                } catch (TTransportException e) {
                    throw new RuntimeException();
                }
            });

    @Override
    public EventPayload convert(byte[] bin, Class<EventPayload> clazz) {
        EventPayload event = new EventPayload();
        try {
            thriftDeserializerThreadLocal.get().deserialize(event, bin);
        } catch (TException e) {
            log.error("BinaryConverterImpl e: ", e);
        }
        return event;
    }
}
