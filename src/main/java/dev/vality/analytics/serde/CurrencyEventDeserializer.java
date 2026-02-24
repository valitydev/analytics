package dev.vality.analytics.serde;

import dev.vality.exrates.events.CurrencyEvent;
import dev.vality.geck.serializer.Geck;
import dev.vality.sink.common.serialization.BinaryDeserializer;
import dev.vality.sink.common.serialization.impl.AbstractThriftBinaryDeserializer;
import org.springframework.stereotype.Component;

@Component
public class CurrencyEventDeserializer extends AbstractThriftBinaryDeserializer<CurrencyEvent> {

    @Override
    public CurrencyEvent deserialize(byte[] bytes) {
        return Geck.msgPackToTBase(bytes, CurrencyEvent.class);
    }
}