package com.rbkmoney.analytics.dao.repository;

import com.rbkmoney.analytics.dao.model.MgAdjustmentRow;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePaymentRepository;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresAdjustmentRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresPaymentRepository;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresRefundRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MgRepositoryFacade {

    private final PostgresPaymentRepository postgresPaymentRepository;
    private final PostgresRefundRepository postgresRefundRepository;
    private final PostgresAdjustmentRepository postgresAdjustmentRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;

    public void insertPayments(List<MgPaymentSinkRow> mgPaymentSinkRows) {
        postgresPaymentRepository.insertBatch(mgPaymentSinkRows);
        clickHousePaymentRepository.insertBatch(mgPaymentSinkRows);
    }

    public void insertRefunds(List<MgRefundRow> mgRefundRows) {
        postgresRefundRepository.insertBatch(mgRefundRows);
        clickHouseRefundRepository.insertBatch(mgRefundRows);
    }

    public void insertAdjustments(List<MgAdjustmentRow> mgAdjustmentRows) {
        postgresAdjustmentRepository.insertBatch(mgAdjustmentRows);
        clickHouseAdjustmentRepository.insertBatch(mgAdjustmentRows);
    }
}
