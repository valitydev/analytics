package dev.vality.analytics.utils;

import java.util.UUID;

public class KeyGenerator {

    public static String generateKey(String prefix) {
        return prefix + UUID.randomUUID();
    }

}
