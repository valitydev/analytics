package dev.vality.analytics.dao.repository.clickhouse;

import dev.vality.analytics.dao.model.WithdrawalRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClickHouseWithdrawalRepository {

    private final JdbcTemplate clickHouseJdbcTemplate;

    @Retryable(retryFor = SQLException.class, backoff = @Backoff(delay = 5000))
    public void insertBatch(List<WithdrawalRow> withdrawalRows) {
        if (CollectionUtils.isEmpty(withdrawalRows)) {
            return;
        }

        log.info("Batch start insert withdrawalRows: {} firstElement: {}",
                withdrawalRows.size(), withdrawalRows.get(0).getWithdrawalId());
        clickHouseJdbcTemplate.batchUpdate(
                ClickHouseWithdrawalBatchPreparedStatementSetter.INSERT,
                new ClickHouseWithdrawalBatchPreparedStatementSetter(withdrawalRows));
        log.info("Batch inserted withdrawalRows: {} firstElement: {}",
                withdrawalRows.size(), withdrawalRows.get(0).getWithdrawalId());
    }
}
