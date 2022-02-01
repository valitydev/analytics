package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.constant.RefundStatus;
import dev.vality.analytics.dao.model.RefundRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.listener.mapper.Mapper;
import dev.vality.analytics.listener.mapper.factory.RowFactory;
import dev.vality.analytics.service.HgClientService;
import dev.vality.damsel.domain.Failure;
import dev.vality.damsel.payment_processing.*;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.serializer.kit.tbase.TErrorUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundMapper implements Mapper<InvoiceChange, MachineEvent, RefundRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;
    private final RowFactory<RefundRow> refundRowFactory;

    @Override
    public RefundRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentRefundChange invoicePaymentRefundChange = invoicePaymentChange.getPayload()
                .getInvoicePaymentRefundChange();
        InvoicePaymentRefundChangePayload payload = invoicePaymentRefundChange.getPayload();
        InvoicePaymentRefundStatusChanged invoicePaymentRefundStatusChanged =
                payload.getInvoicePaymentRefundStatusChanged();
        String refundId = invoicePaymentRefundChange.getId();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(), findPayment(), paymentId, refundId, event.getEventId());
        RefundRow refundRow = refundRowFactory.create(event, invoicePaymentWrapper, refundId);

        refundRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentRefundStatusChanged()
                .getStatus(), RefundStatus.class));

        if (invoicePaymentRefundStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().getFailure();
                refundRow.setErrorCode(TErrorUtil.toStringVal(failure));
                refundRow.setErrorReason(failure.getReason());
            } else if (invoicePaymentRefundStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                refundRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        log.debug("RefundPaymentMapper refundRow: {}", refundRow);
        return refundRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment ->
                        payment.isSetPayment()
                                && payment.isSetRefunds()
                                && payment.getRefunds().stream()
                                .anyMatch(refund -> refund.getRefund().getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_REFUND_STATUS_CHANGED;
    }

}
