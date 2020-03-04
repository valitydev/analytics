package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresRefundRepository {

    private final JdbcTemplate postgresJdbcTemplate;

    public void insertBatch(List<MgRefundRow> mgRefundRows) {
        if (CollectionUtils.isEmpty(mgRefundRows)) return;

        log.info("Batch start insert mgRefundRows: {} firstElement: {}",
                mgRefundRows.size(),
                mgRefundRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                PostgresRefundBatchPreparedStatementSetter.INSERT,
                new PostgresRefundBatchPreparedStatementSetter(mgRefundRows));

        log.info("Batch inserted mgRefundRows: {} firstElement: {}",
                mgRefundRows.size(),
                mgRefundRows.get(0).getInvoiceId());
    }
}