package dev.vality.analytics.utils;

import dev.vality.geck.serializer.Geck;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.xrates.base.TimestampInterval;
import dev.vality.xrates.rate.Change;
import dev.vality.xrates.rate.ExchangeRateCreated;
import dev.vality.xrates.rate.ExchangeRateData;
import dev.vality.xrates.rate.Quote;
import org.apache.thrift.TFieldIdEnum;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.randomListOf;

public class RateSinkEventTestUtils {
    public static void main(String[] args) {
        try {
            List<Quote> quotes = randomListOf(4, Quote.class);
            Change created = Change.created(
                    new ExchangeRateCreated(
                            new ExchangeRateData(
                                    new TimestampInterval(
                                            Instant.now().toString(),
                                            Instant.now().toString()
                                    ),
                                    quotes
                            )
                    )
            );
            TFieldIdEnum[] tFieldIdEnums = created.getFields();
            System.out.println(tFieldIdEnums);
            Geck.toMsgPack(created);
//            create("asd");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static List<SinkEvent> create(String sourceId, String... excludedFields) {
        List<Quote> quotes = randomListOf(4, Quote.class, excludedFields);
        quotes.stream().forEach(quote -> {
            quote.getDestination().setExponent((short) 2);
            quote.getSource().setExponent((short) 2);
            quote.getExchangeRate().setQ(1L);
            quote.getExchangeRate().setP(1L);
        });
        SinkEvent sinkEvent = new SinkEvent();
        Change created = Change.created(
                new ExchangeRateCreated(
                        new ExchangeRateData(
                                new TimestampInterval(
                                        Instant.now().toString(),
                                        Instant.now().toString()
                                ),
                                quotes
                        )
                )
        );
        sinkEvent.setEvent(new MachineEvent()
                .setEventId(123L)
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setSourceId(sourceId)
                .setSourceNs(sourceId)
                .setData(Value.bin(Geck.toMsgPack(
                        created)
                )));
        return Collections.singletonList(sinkEvent);
    }

}
