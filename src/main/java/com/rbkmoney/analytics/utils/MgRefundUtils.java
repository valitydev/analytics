package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static java.time.ZoneOffset.UTC;

public class MgRefundUtils {

    public static void initTimeFields(MgRefundRow mgRefundRow, String createdAt) {
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(createdAt);

        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgRefundRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgRefundRow.setEventTime(timestamp);
        long eventTimeHour = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.HOURS).toEpochMilli();
        mgRefundRow.setEventTimeHour(eventTimeHour);
    }

}
