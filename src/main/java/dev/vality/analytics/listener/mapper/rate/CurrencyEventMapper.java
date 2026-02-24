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

@Component
@Slf4j
public class CurrencyEventMapper {

    public Rate map(CurrencyEvent event) {
        if (!event.isSetPayload()) {
            log.warn("CurrencyEvent payload not set, eventId={}", event.getEventId());
            return null;
        }
        CurrencyEventPayload payload = event.getPayload();
        if (!payload.isSetExchangeRate()) {
            log.warn("CurrencyEvent payload is not exchange_rate, eventId={}", event.getEventId());
            return null;
        }
        CurrencyExchangeRate exchangeRate = payload.getExchangeRate();

        Rate rate = new Rate();
        rate.setEventId(event.getEventId());

        String timestamp = exchangeRate.getTimestamp();
        try {
            rate.setEventTime(TypeUtil.stringToLocalDateTime(timestamp));
        } catch (Exception e) {
            log.warn("Failed to parse timestamp '{}' for CurrencyEvent, eventId={}", timestamp, event.getEventId(), e);
            return null;
        }

        Currency sourceCurrency = exchangeRate.getSourceCurrency();
        Currency destinationCurrency = exchangeRate.getDestinationCurrency();
        Rational rational = exchangeRate.getExchangeRate();

        if (sourceCurrency == null || !sourceCurrency.isSetSymbolicCode() || !sourceCurrency.isSetExponent()) {
            log.warn("CurrencyEvent source currency incomplete, eventId={}", event.getEventId());
            return null;
        }
        if (destinationCurrency == null
                || !destinationCurrency.isSetSymbolicCode()
                || !destinationCurrency.isSetExponent()) {
            log.warn("CurrencyEvent destination currency incomplete, eventId={}", event.getEventId());
            return null;
        }
        if (rational == null || !rational.isSetP() || !rational.isSetQ()) {
            log.warn("CurrencyEvent exchange rate rational incomplete, eventId={}", event.getEventId());
            return null;
        }

        rate.setSourceSymbolicCode(sourceCurrency.getSymbolicCode());
        rate.setSourceExponent(sourceCurrency.getExponent());
        rate.setDestinationSymbolicCode(destinationCurrency.getSymbolicCode());
        rate.setDestinationExponent(destinationCurrency.getExponent());
        rate.setExchangeRateRationalP(rational.getP());
        rate.setExchangeRateRationalQ(rational.getQ());

        return rate;
    }
}
