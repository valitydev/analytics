package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgRefundBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_refund " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, " +
            "amount, currency, providerName, status, errorReason,  invoiceId, " +
            "paymentId, refundId, sequenceId, ip, sign)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgRefundRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgRefundRow mgPaymentSinkRow = batch.get(i);
        int l = 1;
        ps.setDate(l++, mgPaymentSinkRow.getTimestamp());
        ps.setLong(l++, mgPaymentSinkRow.getEventTime());
        ps.setLong(l++, mgPaymentSinkRow.getEventTimeHour());

        ps.setString(l++, mgPaymentSinkRow.getPartyId());
        ps.setString(l++, mgPaymentSinkRow.getShopId());

        ps.setString(l++, mgPaymentSinkRow.getEmail());

        ps.setLong(l++, mgPaymentSinkRow.getAmount());
        ps.setString(l++, mgPaymentSinkRow.getCurrency());

        ps.setString(l++, mgPaymentSinkRow.getProvider());

        ps.setString(l++, mgPaymentSinkRow.getStatus().name());

        ps.setString(l++, mgPaymentSinkRow.getErrorCode());

        ps.setString(l++, mgPaymentSinkRow.getInvoiceId());
        ps.setString(l++, mgPaymentSinkRow.getPaymentId());
        ps.setString(l++, mgPaymentSinkRow.getRefundId());
        ps.setLong(l++, mgPaymentSinkRow.getSequenceId());

        ps.setString(l++, mgPaymentSinkRow.getIp());

        ps.setInt(l, mgPaymentSinkRow.getSign());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
