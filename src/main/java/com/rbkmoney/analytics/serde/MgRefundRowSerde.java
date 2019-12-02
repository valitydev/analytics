package com.rbkmoney.analytics.serde;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class MgRefundRowSerde implements Serde<MgRefundRow> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MgRefundRow> serializer() {
        return new MgRefundRowSerializer();
    }

    @Override
    public Deserializer<MgRefundRow> deserializer() {
        return new MgRefundRowDeserializer();
    }
}
