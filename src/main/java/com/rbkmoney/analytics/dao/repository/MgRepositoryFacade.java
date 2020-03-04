package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MgRepositoryFacade {

    private final PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;

    public void insertPayments(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        postgresBalanceChangesRepository.insertPayments(mgPaymentSinkRows);
        clickHousePaymentRepository.insertBatch(mgPaymentSinkRows);
    }

    public void insertRefunds(List<MgRefundRow> mgRefundRows) {
        postgresBalanceChangesRepository.insertRefunds(mgRefundRows);
        clickHouseRefundRepository.insertBatch(mgRefundRows);
    }

    public void insertAdjustments(List<MgAdjustmentRow> mgAdjustmentRows) {
        postgresBalanceChangesRepository.insertAdjustments(mgAdjustmentRows);
        clickHouseAdjustmentRepository.insertBatch(mgAdjustmentRows);
    }
}
