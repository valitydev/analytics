package com.rbkmoney.analytics.stream.aggregate;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
public class MgPaymentAggregator implements Aggregator<String, MgPaymentSinkRow, MgPaymentSinkRow> {

    @Override
    public MgPaymentSinkRow apply(String key, MgPaymentSinkRow value, MgPaymentSinkRow aggregate) {
        log.debug("Merge aggValue={} and value={}", aggregate, value);
        MgPaymentSinkRow oldEvent = new MgPaymentSinkRow(aggregate);
        aggregate.setOldMgPaymentSinkRow(oldEvent);

        aggregate.setInvoiceId(changeIfNotNull(aggregate.getInvoiceId(), value.getInvoiceId()));
        aggregate.setStatus(value.getStatus());
        aggregate.setPaymentTool(changeIfNotNull(aggregate.getPaymentTool(), value.getPaymentTool()));

        aggregate.setPaymentId(value.getPaymentId());
        aggregate.setEmail(changeIfNotNull(aggregate.getEmail(), value.getEmail()));
        aggregate.setFingerprint(changeIfNotNull(aggregate.getFingerprint(), value.getFingerprint()));
        aggregate.setIp(changeIfNotNull(aggregate.getIp(), value.getIp()));
        aggregate.setProvider(changeIfNotNull(aggregate.getProvider(), value.getProvider()));
        aggregate.setMaskedPan(changeIfNotNull(aggregate.getMaskedPan(), value.getMaskedPan()));
        aggregate.setBin(changeIfNotNull(aggregate.getBin(), value.getBin()));
        aggregate.setBankCountry(changeIfNotNull(aggregate.getBankCountry(), value.getBankCountry()));
        aggregate.setPartyId(changeIfNotNull(aggregate.getPartyId(), value.getPartyId()));
        aggregate.setShopId(changeIfNotNull(aggregate.getShopId(), value.getShopId()));
        aggregate.setErrorCode(changeIfNotNull(aggregate.getErrorCode(), value.getErrorCode()));
        aggregate.setCurrency(changeIfNotNull(aggregate.getCurrency(), value.getCurrency()));
        aggregate.setAmount(changeIfNotNull(aggregate.getAmount(), value.getAmount()));
        aggregate.setIpCountry(changeIfNotNull(aggregate.getIpCountry(), value.getIpCountry()));
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
