package com.rbkmoney.analytics.dao.utils;

import java.sql.Date;
import java.time.Instant;

import static java.time.ZoneOffset.UTC;

public class DateFilterUtils {

    public static Date parseDate(Long to) {
        return Date.valueOf(
                Instant.ofEpochMilli(to)
                        .atZone(UTC)
                        .toLocalDate());
    }

}
