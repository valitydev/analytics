package com.rbkmoney.analytics.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimeUtilsTest {

    @Test
    public void parseEventTimeHour() {
        long l = TimeUtils.parseEventTimeHour(1575666000697L);

        System.out.println(l);
    }

}