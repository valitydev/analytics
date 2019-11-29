package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class MgEventSinkBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink " +
            "(timestamp, eventTime, eventTimeHour, partyId, shopId, email, amount, currency, providerName, status, errorReason,  invoiceId, " +
            "paymentId, sequenceId, ip, bin, maskedPan, paymentTool, sign)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<MgEventSinkRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgEventSinkRow mgEventSinkRow = batch.get(i);
        int l = 1;
        ps.setDate(l++, mgEventSinkRow.getTimestamp());
        ps.setLong(l++, mgEventSinkRow.getEventTime());
        ps.setLong(l++, mgEventSinkRow.getEventTimeHour());

        ps.setString(l++, mgEventSinkRow.getPartyId());
        ps.setString(l++, mgEventSinkRow.getShopId());

        ps.setString(l++, mgEventSinkRow.getEmail());

        ps.setLong(l++, mgEventSinkRow.getAmount());
        ps.setString(l++, mgEventSinkRow.getCurrency());

        ps.setString(l++, mgEventSinkRow.getProvider());

        ps.setString(l++, mgEventSinkRow.getStatus().name());

        ps.setString(l++, mgEventSinkRow.getErrorCode());

        ps.setString(l++, mgEventSinkRow.getInvoiceId());
        ps.setString(l++, mgEventSinkRow.getPaymentId());
        ps.setLong(l++, mgEventSinkRow.getSequenceId());

        ps.setString(l++, mgEventSinkRow.getIp());
        ps.setString(l++, mgEventSinkRow.getBin());
        ps.setString(l++, mgEventSinkRow.getMaskedPan());
        ps.setString(l++, mgEventSinkRow.getPaymentTool().name());

        ps.setInt(l, mgEventSinkRow.getSign());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
