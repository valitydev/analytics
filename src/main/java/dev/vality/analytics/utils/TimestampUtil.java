package dev.vality.analytics.utils;

import dev.vality.geck.common.util.TypeUtil;
import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;

@UtilityClass
public class TimestampUtil {

    public static LocalDateTime parseLocalDateTime(String timestamp) {
        if (timestamp == null) {
            return null;
        }

        try {
            return TypeUtil.stringToLocalDateTime(timestamp);
        } catch (Exception e) {
            return null;
        }
    }

}
