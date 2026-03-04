package dev.vality.analytics.dao.repository.clickhouse;

import dev.vality.analytics.constant.ClickHouseUtilsValue;
import dev.vality.analytics.dao.model.WithdrawalRow;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class ClickHouseWithdrawalBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_withdrawal " +
            "(timestamp, eventTime, eventTimeHour, partyId, withdrawalId, sequenceId, withdrawalTime, " +
            "walletId, " +
            "destinationId, providerId, terminal, amount, guaranteeDeposit, systemFee, providerFee, " +
            "externalFee, " +
            "currency, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<WithdrawalRow> batch;

    public ClickHouseWithdrawalBatchPreparedStatementSetter(List<WithdrawalRow> batch) {
        this.batch = batch;
    }

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        WithdrawalRow row = batch.get(i);
        int column = 1;
        ps.setObject(column++, row.getEventTime().toLocalDate());
        ps.setLong(column++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(
                column++,
                row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setString(column++, defaultString(row.getPartyId()));
        ps.setString(column++, defaultString(row.getWithdrawalId()));
        ps.setLong(column++, row.getSequenceId());
        ps.setLong(column++, row.getWithdrawalTime().toEpochSecond(ZoneOffset.UTC));
        ps.setString(column++, defaultString(row.getWalletId()));
        ps.setString(column++, defaultString(row.getDestinationId()));
        ps.setString(column++, defaultString(row.getProviderId()));
        ps.setString(column++, defaultString(row.getTerminal()));
        ps.setLong(column++, safeUnsigned(row.getAmount()));
        ps.setLong(column++, safeUnsigned(row.getGuaranteeDeposit()));
        ps.setLong(column++, safeUnsigned(row.getSystemFee()));
        ps.setLong(column++, safeUnsigned(row.getProviderFee()));
        ps.setLong(column++, safeUnsigned(row.getExternalFee()));
        ps.setString(column++, defaultString(row.getCurrency()));
        ps.setString(column, row.getStatus().name());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }

    private String defaultString(String value) {
        return value != null ? value : ClickHouseUtilsValue.UNKNOWN;
    }

    private long safeUnsigned(long value) {
        return Math.max(0L, value);
    }
}
