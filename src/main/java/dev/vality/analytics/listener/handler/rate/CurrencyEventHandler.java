package dev.vality.analytics.listener.handler.rate;

import dev.vality.analytics.dao.repository.postgres.RateDao;
import dev.vality.analytics.domain.db.tables.pojos.Rate;
import dev.vality.analytics.listener.mapper.rate.CurrencyEventMapper;
import dev.vality.exrates.events.CurrencyEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurrencyEventHandler {

    private final CurrencyEventMapper currencyEventMapper;
    private final RateDao rateDao;
    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handle(
            final List<CurrencyEvent> batch,
            final Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) {
                ack.acknowledge();
                return;
            }

            List<Rate> rates = batch.stream()
                    .map(currencyEventMapper::map)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (!rates.isEmpty()) {
                rateDao.saveRateBatch(rates);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during CurrencyEventHandler process", e);
            ack.nack(Duration.ofMillis(throttlingTimeout));
            throw e;
        }
    }
}
