package com.rbkmoney.analytics.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class MgPaymentRowDeserializer implements Deserializer<MgPaymentSinkRow> {

    private final ObjectMapper om = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public MgPaymentSinkRow deserialize(String topic, byte[] data) {
        MgPaymentSinkRow mgPaymentSinkRow = null;
        try {
            mgPaymentSinkRow = om.readValue(data, MgPaymentSinkRow.class);
        } catch (Exception e) {
            log.error("Error when deserialize MgEventSinkRow data: {} ", data, e);
        }
        return mgPaymentSinkRow;
    }

    @Override
    public void close() {

    }

}
