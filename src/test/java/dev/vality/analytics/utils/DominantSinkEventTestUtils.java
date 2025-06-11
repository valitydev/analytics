package dev.vality.analytics.utils;

import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import dev.vality.geck.serializer.Geck;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.machinegun.msgpack.Value;

import java.util.List;

public class DominantSinkEventTestUtils {

    public static final Author CHANGED_BY = new Author("test_id", "test_email", "test_name");

    public static SinkEvent create(FinalOperation finalOperation, long version) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(new MachineEvent()
                .setEventId(123L)
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setSourceId("test")
                .setSourceNs("test")
                .setData(Value.bin(Geck.toMsgPack(new HistoricalCommit()
                                        .setCreatedAt("2016-03-22T06:12:27Z")
                                        .setOps(List.of(finalOperation))
                                        .setChangedBy(CHANGED_BY)
                                        .setVersion(version)
                                )
                        )
                )
        );
        return sinkEvent;
    }
}
