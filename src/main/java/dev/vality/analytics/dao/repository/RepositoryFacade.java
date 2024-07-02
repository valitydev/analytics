package dev.vality.analytics.dao.repository;

import dev.vality.analytics.constant.AdjustmentStatus;
import dev.vality.analytics.constant.ChargebackStatus;
import dev.vality.analytics.constant.PaymentStatus;
import dev.vality.analytics.constant.RefundStatus;
import dev.vality.analytics.dao.model.AdjustmentRow;
import dev.vality.analytics.dao.model.ChargebackRow;
import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.dao.model.RefundRow;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseAdjustmentRepository;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseChargebackRepository;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseRefundRepository;
import dev.vality.analytics.dao.repository.clickhouse.iface.ClickHousePaymentRepository;
import dev.vality.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class RepositoryFacade {

    private final PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    private final ClickHousePaymentRepository clickHousePaymentRepository;
    private final ClickHouseRefundRepository clickHouseRefundRepository;
    private final ClickHouseAdjustmentRepository clickHouseAdjustmentRepository;
    private final ClickHouseChargebackRepository clickHouseChargebackRepository;

    public void insertPayments(List<PaymentRow> paymentRows) {
        List<PaymentRow> capturedPayments = paymentRows.stream()
                .filter(paymentRow -> paymentRow.getStatus() == PaymentStatus.captured)
                .collect(toList());

        postgresBalanceChangesRepository.insertPayments(capturedPayments);
        log.info("RepositoryFacade PG inserted insertPayments: {}", capturedPayments.size());
        clickHousePaymentRepository.insertBatch(paymentRows);
        log.info("RepositoryFacade CH inserted insertPayments: {}", paymentRows.size());
    }

    public void insertRefunds(List<RefundRow> refundRows) {
        List<RefundRow> succeededRefunds = refundRows.stream()
                .filter(refundRow -> refundRow.getStatus() == RefundStatus.succeeded)
                .collect(toList());

        postgresBalanceChangesRepository.insertRefunds(succeededRefunds);
        log.info("RepositoryFacade PG inserted insertRefunds: {}", succeededRefunds.size());
        clickHouseRefundRepository.insertBatch(refundRows);
        log.info("RepositoryFacade CH inserted insertRefunds: {}", refundRows.size());
    }

    public void insertAdjustments(List<AdjustmentRow> adjustmentRows) {
        List<AdjustmentRow> capturedAdjustments = adjustmentRows.stream()
                .filter(adjustmentRow -> adjustmentRow.getStatus() == AdjustmentStatus.captured)
                .collect(toList());

        postgresBalanceChangesRepository.insertAdjustments(capturedAdjustments);
        log.info("RepositoryFacade PG inserted insertAdjustments: {}", capturedAdjustments.size());
        clickHouseAdjustmentRepository.insertBatch(adjustmentRows);
        log.info("RepositoryFacade CH inserted insertAdjustments: {}", adjustmentRows.size());
    }

    public void insertChargebacks(List<ChargebackRow> chargebackRows) {
        List<ChargebackRow> acceptedChargebacks = chargebackRows.stream()
                .filter(chargebackRow -> chargebackRow.getStatus() == ChargebackStatus.accepted)
                .collect(toList());

        postgresBalanceChangesRepository.insertChargebacks(acceptedChargebacks);
        log.info("RepositoryFacade PG inserted insertChargebacks: {}", acceptedChargebacks.size());
        clickHouseChargebackRepository.insertBatch(chargebackRows);
        log.info("RepositoryFacade CH inserted insertChargebacks: {}", chargebackRows.size());
    }
}
