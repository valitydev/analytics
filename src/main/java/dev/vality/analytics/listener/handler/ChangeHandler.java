package dev.vality.analytics.listener.handler;

import dev.vality.analytics.constant.EventType;

public interface ChangeHandler<C, P> {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    void handleChange(C change, P parent);

    EventType getChangeType();

}
