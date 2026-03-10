package dev.vality.analytics.config;

import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.exrates.events.CurrencyEvent;
import dev.vality.geck.serializer.Geck;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import dev.vality.sink.common.serialization.impl.AbstractThriftBinaryDeserializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerializeConfig {

    @Bean
    public BinaryDeserializer<CurrencyEvent> currencyEventBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public CurrencyEvent deserialize(byte[] bytes) {
                return Geck.msgPackToTBase(bytes, CurrencyEvent.class);
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

    @Bean
    public BinaryDeserializer<TimestampedChange> withdrawalTimestampedChangeBinaryDeserializer() {
        return new AbstractThriftBinaryDeserializer<>() {
            @Override
            public TimestampedChange deserialize(byte[] bytes) {
                return deserialize(bytes, new TimestampedChange());
            }
        };
    }

    @Bean
    public MachineEventParser<TimestampedChange> withdrawalTimestampedChangeMachineEventParser(
            BinaryDeserializer<TimestampedChange> withdrawalTimestampedChangeBinaryDeserializer) {
        return new MachineEventParser<>(withdrawalTimestampedChangeBinaryDeserializer);
    }

}
