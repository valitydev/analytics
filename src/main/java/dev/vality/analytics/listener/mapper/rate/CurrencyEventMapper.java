package dev.vality.analytics.listener.mapper.rate;

import dev.vality.analytics.domain.db.tables.pojos.Rate;
import dev.vality.exrates.base.Currency;
import dev.vality.exrates.base.Rational;
import dev.vality.exrates.events.CurrencyEvent;
import dev.vality.exrates.events.CurrencyEventPayload;
import dev.vality.exrates.events.CurrencyExchangeRate;
import dev.vality.geck.common.util.TypeUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class CurrencyEventMapper {

    public Rate map(CurrencyEvent event) {
        String eventId = event.getEventId();
        CurrencyExchangeRate exchangeRate = validateAndGetExchangeRate(event, eventId);
        if (exchangeRate == null) {
            return null;
        }

        Currency sourceCurrency = exchangeRate.getSourceCurrency();
        Currency destinationCurrency = exchangeRate.getDestinationCurrency();
        Rational rational = exchangeRate.getExchangeRate();
        var eventTime = parseEventTime(exchangeRate.getTimestamp(), eventId);
        if (eventTime == null) {
            return null;
        }

        return buildRate(eventId, eventTime, sourceCurrency, destinationCurrency, rational);
    }

    private CurrencyExchangeRate validateAndGetExchangeRate(CurrencyEvent event, String eventId) {
        if (!event.isSetPayload()) {
            log.warn("CurrencyEvent payload not set, eventId={}", eventId);
            return null;
        }

        CurrencyEventPayload payload = event.getPayload();
        if (!payload.isSetExchangeRate()) {
            log.warn("CurrencyEvent payload is not exchange_rate, eventId={}", eventId);
            return null;
        }

        CurrencyExchangeRate exchangeRate = payload.getExchangeRate();
        Currency sourceCurrency = exchangeRate.getSourceCurrency();
        if (!isCurrencySet(sourceCurrency)) {
            log.warn("CurrencyEvent source currency incomplete, eventId={}", eventId);
            return null;
        }

        Currency destinationCurrency = exchangeRate.getDestinationCurrency();
        if (!isCurrencySet(destinationCurrency)) {
            log.warn("CurrencyEvent destination currency incomplete, eventId={}", eventId);
            return null;
        }

        Rational rational = exchangeRate.getExchangeRate();
        if (!isRationalSet(rational)) {
            log.warn("CurrencyEvent exchange rate rational incomplete, eventId={}", eventId);
            return null;
        }

        return exchangeRate;
    }

    private LocalDateTime parseEventTime(String timestamp, String eventId) {
        try {
            return TypeUtil.stringToLocalDateTime(timestamp);
        } catch (Exception e) {
            log.warn("Failed to parse timestamp '{}' for CurrencyEvent, eventId={}", timestamp, eventId, e);
            return null;
        }
    }

    private Rate buildRate(
            String eventId,
            LocalDateTime eventTime,
            Currency sourceCurrency,
            Currency destinationCurrency,
            Rational rational) {
        Rate rate = new Rate();
        rate.setEventId(eventId);
        rate.setEventTime(eventTime);
        rate.setSourceSymbolicCode(sourceCurrency.getSymbolicCode());
        rate.setSourceExponent(sourceCurrency.getExponent());
        rate.setDestinationSymbolicCode(destinationCurrency.getSymbolicCode());
        rate.setDestinationExponent(destinationCurrency.getExponent());
        rate.setExchangeRateRationalP(rational.getP());
        rate.setExchangeRateRationalQ(rational.getQ());
        return rate;
    }

    private boolean isCurrencySet(Currency currency) {
        return currency != null && currency.isSetSymbolicCode() && currency.isSetExponent();
    }

    private boolean isRationalSet(Rational rational) {
        return rational != null && rational.isSetP() && rational.isSetQ();
    }
}
