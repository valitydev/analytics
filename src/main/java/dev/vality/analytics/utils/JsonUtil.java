package dev.vality.analytics.utils;

import dev.vality.geck.serializer.kit.json.JsonHandler;
import dev.vality.geck.serializer.kit.tbase.TBaseProcessor;
import org.apache.thrift.TBase;

import java.io.IOException;

public class JsonUtil {

    public static String thriftBaseToJsonString(TBase thriftBase) {
        try {
            return new TBaseProcessor().process(thriftBase, new JsonHandler()).toString();
        } catch (IOException e) {
            throw new RuntimeException("Couldn't convert to json string: " + thriftBase, e);
        }
    }

}
