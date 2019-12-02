package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgPaymentRepository {

    private final JdbcTemplate jdbcTemplate;

    public void insertBatch(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        if (mgPaymentSinkRows != null && !mgPaymentSinkRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgPaymentBatchPreparedStatementSetter.INSERT, new MgPaymentBatchPreparedStatementSetter(mgPaymentSinkRows));
        }
    }

}
