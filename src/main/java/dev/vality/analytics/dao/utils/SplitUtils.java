package dev.vality.analytics.dao.utils;

import dev.vality.damsel.analytics.SplitUnit;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

import static java.time.ZoneOffset.UTC;

public class SplitUtils {

    public static final String HOUR = "hour";
    public static final String MINUTES = "minutes";
    public static final String DAY = "day";
    public static final String WEEK = "week";
    public static final String YEAR = "year";
    public static final String MONTHS = "months";

    public static Long generateOffset(Map row, SplitUnit splitUnit) {
        switch (splitUnit) {
            case MINUTE -> {
                var hour = (Short) row.get(HOUR);
                var minutes = (Short) row.get(MINUTES);
                var date = (Date) row.get(DAY);
                var localDateTime = date.toLocalDate()
                        .atStartOfDay()
                        .plusHours(hour)
                        .plusMinutes(minutes);
                return localDateTime
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case HOUR -> {
                var hour = (Short) row.get(HOUR);
                var date = (Date) row.get(DAY);
                var localDateTime = date.toLocalDate()
                        .atStartOfDay()
                        .plusHours(hour);
                return localDateTime
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case DAY -> {
                var date = (Date) row.get(DAY);
                return date.toLocalDate()
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case WEEK -> {
                var date = (Date) row.get(WEEK);
                return date.toLocalDate()
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case MONTH -> {
                var date = (Integer) row.get(YEAR);
                var months = (Short) row.get(MONTHS);
                return LocalDate.of(date, months, 1)
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            case YEAR -> {
                var date = (Integer) row.get(YEAR);
                return LocalDate.of(date, 1, 1)
                        .atStartOfDay()
                        .atZone(UTC)
                        .toInstant().toEpochMilli();
            }
            default -> throw new RuntimeException();
        }
    }

    public static String initGroupByFunction(SplitUnit splitUnit) {
        return switch (splitUnit) {
            case MINUTE -> "timestamp as day, toHour(toDateTime(eventTime, 'UTC')) as hour, " +
                    "toMinute(toDateTime(eventTime, 'UTC')) as minutes";
            case HOUR -> "timestamp as day, toHour(toDateTime(eventTime, 'UTC')) as hour";
            case DAY -> "timestamp as day";
            case WEEK -> "toStartOfWeek(timestamp, 1) as week";
            case MONTH -> "toYear(timestamp) as year, toMonth(timestamp) as months";
            case YEAR -> "toYear(timestamp) as year";
        };
    }


}
