package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgRefundRepository {

    private final JdbcTemplate jdbcTemplate;

    public void insertBatch(List<MgRefundRow> mgRefundRows) {
        if (mgRefundRows != null && !mgRefundRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgPaymentBatchPreparedStatementSetter.INSERT, new MgRefundBatchPreparedStatementSetter(mgRefundRows));
        }
    }

}
