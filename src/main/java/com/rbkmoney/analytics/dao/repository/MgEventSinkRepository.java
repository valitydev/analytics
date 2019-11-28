package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MgEventSinkRepository {

    private final JdbcTemplate jdbcTemplate;

    public void insertBatch(List<MgEventSinkRow> mgEventSinkRows) {
        if (mgEventSinkRows != null && !mgEventSinkRows.isEmpty()) {
            jdbcTemplate.batchUpdate(MgEventSinkBatchPreparedStatementSetter.INSERT, new MgEventSinkBatchPreparedStatementSetter(mgEventSinkRows));
        }
    }

}
