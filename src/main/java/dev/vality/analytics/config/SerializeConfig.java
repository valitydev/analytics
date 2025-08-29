package dev.vality.analytics.config;

import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.geck.serializer.Geck;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import dev.vality.sink.common.serialization.impl.AbstractThriftBinaryDeserializer;
import dev.vality.xrates.rate.Change;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerializeConfig {

    @Bean
    public BinaryDeserializer<Change> rateEventDataBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public Change deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, Change.class);
            }
        };
    }

    @Bean
    public MachineEventParser<HistoricalCommit> historicalCommitMachineEventParser() {
        return new MachineEventParser<>(new AbstractThriftBinaryDeserializer<>() {
            @Override
            public HistoricalCommit deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, HistoricalCommit.class);
            }
        });
    }

}
