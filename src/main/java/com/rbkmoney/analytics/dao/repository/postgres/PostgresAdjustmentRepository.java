package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresAdjustmentRepository {

    private final JdbcTemplate postgresJdbcTemplate;

    public void insertBatch(List<MgAdjustmentRow> adjustmentRows) {
        if (CollectionUtils.isEmpty(adjustmentRows)) return;

        log.info("Batch start insert adjustmentRows: {} firstElement: {}",
                adjustmentRows.size(),
                adjustmentRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                PostgresAdjustmentBatchPreparedStatementSetter.INSERT,
                new PostgresAdjustmentBatchPreparedStatementSetter(adjustmentRows));

        log.info("Batch inserted adjustmentRows: {} firstElement: {}",
                adjustmentRows.size(),
                adjustmentRows.get(0).getInvoiceId());
    }

}
