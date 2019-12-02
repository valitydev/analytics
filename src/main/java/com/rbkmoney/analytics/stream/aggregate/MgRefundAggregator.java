package com.rbkmoney.analytics.stream.aggregate;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
public class MgRefundAggregator implements Aggregator<String, MgRefundRow, MgRefundRow> {

    @Override
    public MgRefundRow apply(String key, MgRefundRow value, MgRefundRow aggregate) {
        log.debug("Merge aggValue={} and value={}", aggregate, value);
        MgRefundRow oldEvent = new MgRefundRow(aggregate);
        aggregate.setOldMgRefundRow(oldEvent);

        aggregate.setInvoiceId(changeIfNotNull(aggregate.getInvoiceId(), value.getInvoiceId()));
        aggregate.setStatus(value.getStatus());

        aggregate.setPaymentId(value.getPaymentId());
        aggregate.setRefundId(value.getRefundId());
        aggregate.setEmail(changeIfNotNull(aggregate.getEmail(), value.getEmail()));
        aggregate.setFingerprint(changeIfNotNull(aggregate.getFingerprint(), value.getFingerprint()));
        aggregate.setIp(changeIfNotNull(aggregate.getIp(), value.getIp()));
        aggregate.setProvider(changeIfNotNull(aggregate.getProvider(), value.getProvider()));
        aggregate.setPartyId(changeIfNotNull(aggregate.getPartyId(), value.getPartyId()));
        aggregate.setShopId(changeIfNotNull(aggregate.getShopId(), value.getShopId()));
        aggregate.setErrorCode(changeIfNotNull(aggregate.getErrorCode(), value.getErrorCode()));
        aggregate.setCurrency(changeIfNotNull(aggregate.getCurrency(), value.getCurrency()));
        aggregate.setAmount(changeIfNotNull(aggregate.getAmount(), value.getAmount()));
        aggregate.setTimestamp(changeIfNotNull(aggregate.getTimestamp(), value.getTimestamp()));
        aggregate.setEventTime(changeIfNotNull(aggregate.getEventTime(), value.getEventTime()));
        aggregate.setEventTimeHour(changeIfNotNull(aggregate.getEventTimeHour(), value.getEventTimeHour()));

        aggregate.setSequenceId(value.getSequenceId());

        log.debug("Merge result={}", aggregate);
        return aggregate;
    }

    private String changeIfNotNull(String value, String newValue) {
        return StringUtils.isEmpty(value) ? newValue : value;
    }

    private <T> T changeIfNotNull(T value, T newValue) {
        return value == null ? newValue : value;
    }


}
