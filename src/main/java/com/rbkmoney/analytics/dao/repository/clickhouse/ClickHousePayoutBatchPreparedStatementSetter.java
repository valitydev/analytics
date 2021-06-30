package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.constant.ClickHouseUtilsValue;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class ClickHousePayoutBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_payout " +
            "(payoutId, status, payoutToolId, statusCancelledDetails, isCancelledAfterBeingPaid, timestamp, " +
            "eventTime, eventTimeHour, payoutTime, shopId, partyId, amount, fee, currency)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<PayoutRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        PayoutRow row = batch.get(i);
        int l = 1;

        ps.setString(l++, row.getPayoutId());
        ps.setString(l++, row.getStatus().name());
        ps.setString(l++, row.getPayoutId());
        ps.setString(l++, row.getStatusCancelledDetails());
        ps.setBoolean(l++, row.isCancelledAfterBeingPaid());

        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setLong(l++, row.getPayoutTime().toEpochSecond(ZoneOffset.UTC));

        ps.setString(l++, row.getShopId());
        ps.setString(l++, row.getPartyId());

        ps.setLong(l++, row.getAmount());
        ps.setLong(l++, row.getFee());
        ps.setString(l++, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
