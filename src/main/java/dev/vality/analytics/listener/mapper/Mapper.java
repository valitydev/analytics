package dev.vality.analytics.listener.mapper;

import dev.vality.analytics.constant.EventType;

public interface Mapper<C, P, R> {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    R map(C change, P parent);

    EventType getChangeType();

}
