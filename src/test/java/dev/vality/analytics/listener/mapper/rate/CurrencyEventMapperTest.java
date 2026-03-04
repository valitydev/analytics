package dev.vality.analytics.listener.mapper.rate;

import dev.vality.analytics.domain.db.tables.pojos.Rate;
import dev.vality.exrates.base.Currency;
import dev.vality.exrates.base.Rational;
import dev.vality.exrates.events.CurrencyEvent;
import dev.vality.exrates.events.CurrencyEventPayload;
import dev.vality.exrates.events.CurrencyExchangeRate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class CurrencyEventMapperTest {

    private CurrencyEventMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new CurrencyEventMapper();
    }

    @Test
    void mapValidEvent() {
        CurrencyEvent event = createValidCurrencyEvent("event1", "USD", "RUB", 75L, 1L,
                Instant.parse("2024-01-01T12:00:00Z"));

        Rate rate = mapper.map(event);

        assertNotNull(rate);
        assertEquals("event1", rate.getEventId());
        assertEquals(LocalDateTime.of(2024, 1, 1, 12, 0, 0), rate.getEventTime());
        assertEquals("USD", rate.getSourceSymbolicCode());
        assertEquals((short) 2, rate.getSourceExponent());
        assertEquals("RUB", rate.getDestinationSymbolicCode());
        assertEquals((short) 2, rate.getDestinationExponent());
        assertEquals(75L, rate.getExchangeRateRationalP());
        assertEquals(1L, rate.getExchangeRateRationalQ());
    }

    @Test
    void mapEventMissingPayload() {
        CurrencyEvent event = new CurrencyEvent();
        event.setEventId("event1");

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    @Test
    void mapEventPayloadNotExchangeRate() {
        CurrencyEvent event = new CurrencyEvent();
        event.setEventId("event1");
        CurrencyEventPayload payload = new CurrencyEventPayload();
        // payload not set
        event.setPayload(payload);

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    @Test
    void mapEventMissingSourceCurrency() {
        CurrencyEvent event = createValidCurrencyEvent("event1", "USD", "RUB", 75L, 1L,
                Instant.parse("2024-01-01T12:00:00Z"));
        // Clear source currency
        event.getPayload().getExchangeRate().unsetSourceCurrency();

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    @Test
    void mapEventMissingDestinationCurrency() {
        CurrencyEvent event = createValidCurrencyEvent("event1", "USD", "RUB", 75L, 1L,
                Instant.parse("2024-01-01T12:00:00Z"));
        event.getPayload().getExchangeRate().unsetDestinationCurrency();

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    @Test
    void mapEventMissingRational() {
        CurrencyEvent event = createValidCurrencyEvent("event1", "USD", "RUB", 75L, 1L,
                Instant.parse("2024-01-01T12:00:00Z"));
        event.getPayload().getExchangeRate().unsetExchangeRate();

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    @Test
    void mapEventInvalidTimestamp() {
        CurrencyEvent event = createValidCurrencyEvent("event1", "USD", "RUB", 75L, 1L,
                Instant.parse("2024-01-01T12:00:00Z"));
        event.getPayload().getExchangeRate().setTimestamp("not-a-valid-timestamp");

        Rate rate = mapper.map(event);

        assertNull(rate);
    }

    private CurrencyEvent createValidCurrencyEvent(String eventId, String sourceCode, String destCode,
                                                   long p, long q, Instant timestamp) {
        Currency sourceCurrency = new Currency();
        sourceCurrency.setSymbolicCode(sourceCode);
        sourceCurrency.setExponent((short) 2);

        Currency destCurrency = new Currency();
        destCurrency.setSymbolicCode(destCode);
        destCurrency.setExponent((short) 2);

        Rational rational = new Rational();
        rational.setP(p);
        rational.setQ(q);

        CurrencyExchangeRate exchangeRate = new CurrencyExchangeRate();
        exchangeRate.setSourceCurrency(sourceCurrency);
        exchangeRate.setDestinationCurrency(destCurrency);
        exchangeRate.setExchangeRate(rational);
        exchangeRate.setTimestamp(timestamp.toString());

        CurrencyEventPayload payload = new CurrencyEventPayload();
        payload.setExchangeRate(exchangeRate);

        CurrencyEvent event = new CurrencyEvent();
        event.setEventId(eventId);
        event.setPayload(payload);

        return event;
    }
}