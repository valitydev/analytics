package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@RequiredArgsConstructor
public class PostgresPaymentBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<MgPaymentSinkRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        MgPaymentSinkRow row = batch.get(i);
        int l = 1;
        ps.setDate(l++, row.getTimestamp());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopId());

        CashFlowResult cashFlowResult = row.getCashFlowResult();
        if (cashFlowResult != null) {
            ps.setLong(l++, cashFlowResult.getAmount() - cashFlowResult.getSystemFee());
        }

        ps.setString(l, row.getCurrency());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}