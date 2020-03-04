package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresPaymentRepository {

    private final JdbcTemplate postgresJdbcTemplate;

    public void insertBatch(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (CollectionUtils.isEmpty(mgPaymentSinkRows)) return;

        log.info("Batch start insert mgPaymentSinkRows: {} firstElement: {}",
                mgPaymentSinkRows.size(),
                mgPaymentSinkRows.get(0).getInvoiceId());

        postgresJdbcTemplate.batchUpdate(
                PostgresPaymentBatchPreparedStatementSetter.INSERT,
                new PostgresPaymentBatchPreparedStatementSetter(mgPaymentSinkRows));

        log.info("Batch inserted mgPaymentSinkRows: {} firstElement: {}",
                mgPaymentSinkRows.size(),
                mgPaymentSinkRows.get(0).getInvoiceId());
    }
}