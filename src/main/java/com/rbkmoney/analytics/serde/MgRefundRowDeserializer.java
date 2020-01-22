package com.rbkmoney.analytics.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class MgRefundRowDeserializer implements Deserializer<MgRefundRow> {

    private final ObjectMapper om = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public MgRefundRow deserialize(String topic, byte[] data) {
        MgRefundRow mgEventSinkRow = null;
        try {
            mgEventSinkRow = om.readValue(data, MgRefundRow.class);
        } catch (Exception e) {
            log.error("Error when deserialize MgEventSinkRow data: {} ", data, e);
        }
        return mgEventSinkRow;
    }

    @Override
    public void close() {

    }

}
