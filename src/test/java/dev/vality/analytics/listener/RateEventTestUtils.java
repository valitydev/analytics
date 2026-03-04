package dev.vality.analytics.listener;

import dev.vality.exrates.base.Currency;
import dev.vality.exrates.base.Rational;
import dev.vality.exrates.events.CurrencyEvent;
import dev.vality.exrates.events.CurrencyEventPayload;
import dev.vality.exrates.events.CurrencyExchangeRate;
import dev.vality.geck.common.util.TypeUtil;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RateEventTestUtils {

    public static CurrencyEvent createCurrencyEvent(String eventId, String sourceCode, String destCode,
                                                    long p, long q, Instant timestamp) {
        Currency sourceCurrency = new Currency()
                .setSymbolicCode(sourceCode)
                .setExponent((short) 2);
        Currency destCurrency = new Currency()
                .setSymbolicCode(destCode)
                .setExponent((short) 2);
        Rational rational = new Rational()
                .setP(p)
                .setQ(q);
        CurrencyExchangeRate exchangeRate = new CurrencyExchangeRate();
        exchangeRate.setSourceCurrency(sourceCurrency);
        exchangeRate.setDestinationCurrency(destCurrency);
        exchangeRate.setExchangeRate(rational);
        exchangeRate.setTimestamp(TypeUtil.temporalToString(timestamp));
        CurrencyEventPayload payload = new CurrencyEventPayload();
        payload.setExchangeRate(exchangeRate);
        CurrencyEvent event = new CurrencyEvent();
        event.setEventId(eventId);
        event.setEventCreatedAt(TypeUtil.temporalToString(timestamp));
        event.setPayload(payload);
        return event;
    }

    public static List<CurrencyEvent> createCurrencyEvents(int count, Instant baseTime) {
        return IntStream.range(0, count)
                .mapToObj(i -> createCurrencyEvent(
                        "event_" + i,
                        "USD",
                        "RUB",
                        75L + i,
                        1L,
                        baseTime.plusSeconds(i * 3600)))
                .collect(Collectors.toList());
    }
}