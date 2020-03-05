package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseAdjustmentRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    public void insertBatch(List<MgAdjustmentRow> adjustmentRows) {
        if (adjustmentRows != null && !adjustmentRows.isEmpty()) {
            log.info("Batch start insert adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
            clickHouseJdbcTemplate.batchUpdate(ClickHouseAdjustmentBatchPreparedStatementSetter.INSERT,
                    new ClickHouseAdjustmentBatchPreparedStatementSetter(adjustmentRows));
            log.info("Batch inserted adjustmentRows: {} firstElement: {}", adjustmentRows.size(),
                    adjustmentRows.get(0).getInvoiceId());
        }
    }

}
