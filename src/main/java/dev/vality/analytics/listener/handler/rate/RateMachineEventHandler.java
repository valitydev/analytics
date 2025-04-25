package dev.vality.analytics.listener.handler.rate;

import dev.vality.analytics.dao.repository.postgres.RateDao;
import dev.vality.analytics.domain.db.tables.pojos.Rate;
import dev.vality.analytics.listener.mapper.Mapper;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import dev.vality.xrates.rate.Change;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RateMachineEventHandler {

    private final MachineEventParser<Change> eventParser;
    private final List<Mapper<Change, MachineEvent, List<Rate>>> mappers;
    private final RateDao rateDao;
    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handle(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) {
                return;
            }

            for (MachineEvent machineEvent : batch) {
                final Change change = eventParser.parse(machineEvent);
                final List<Rate> rates = mappers.stream()
                        .filter(mapper -> mapper.accept(change))
                        .map(mapper -> mapper.map(change, machineEvent))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
                rateDao.saveRateBatch(rates);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            ack.nack(Duration.ofMillis(throttlingTimeout));
            throw e;
        }
    }
}
