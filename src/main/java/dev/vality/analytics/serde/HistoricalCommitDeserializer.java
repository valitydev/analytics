package dev.vality.analytics.serde;

import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;

@Slf4j
public class HistoricalCommitDeserializer implements Deserializer<HistoricalCommit> {

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
    public HistoricalCommit deserialize(String topic, byte[] data) {
        log.debug("Message, topic: {}, byteLength: {}", topic, data.length);
        HistoricalCommit historicalCommit = new HistoricalCommit();
        try {
            thriftDeserializerThreadLocal.get().deserialize(historicalCommit, data);
        } catch (Exception e) {
            log.error("Error when deserialize ruleTemplate data: {} ", data, e);
        }
        return historicalCommit;
    }

    @Override
    public void close() {
    }
}
