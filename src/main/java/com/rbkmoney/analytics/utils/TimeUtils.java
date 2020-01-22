package com.rbkmoney.analytics.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

    public static long parseEventTimeHour(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .truncatedTo(ChronoUnit.HOURS).toEpochMilli();
    }

}
