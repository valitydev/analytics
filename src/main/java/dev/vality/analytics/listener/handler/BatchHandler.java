package dev.vality.analytics.listener.handler;

import dev.vality.analytics.listener.Processor;
import dev.vality.analytics.listener.mapper.Mapper;

import java.util.List;
import java.util.Map;

public interface BatchHandler<C, P> {

    default boolean accept(C change) {
        return getMappers().stream().anyMatch(mapper -> mapper.accept(change));
    }

    Processor handle(List<Map.Entry<P, C>> changes);

    <T> List<Mapper<C, P, T>> getMappers();

}
