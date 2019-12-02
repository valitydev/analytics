package com.rbkmoney.analytics.serde;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class MgPaymentRowSerde implements Serde<MgPaymentSinkRow> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MgPaymentSinkRow> serializer() {
        return new MgPaymentRowSerializer();
    }

    @Override
    public Deserializer<MgPaymentSinkRow> deserializer() {
        return new MgPaymentRowDeserializer();
    }
}
