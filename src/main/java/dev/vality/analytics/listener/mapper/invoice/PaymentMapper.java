package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.constant.PaymentStatus;
import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.listener.mapper.AbstractMapper;
import dev.vality.analytics.listener.mapper.factory.RowFactory;
import dev.vality.analytics.service.HgClientService;
import dev.vality.damsel.domain.Failure;
import dev.vality.damsel.domain.OperationFailure;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChangePayload;
import dev.vality.damsel.payment_processing.InvoicePaymentStatusChanged;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.serializer.kit.tbase.TErrorUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentMapper extends AbstractMapper<InvoiceChange, MachineEvent, PaymentRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    private final HgClientService hgClientService;
    private final RowFactory<PaymentRow> paymentSinkRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change)
                && (change
                .getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetFailed()
                || change
                .getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCancelled()
                || change
                .getInvoicePaymentChange().getPayload().getInvoicePaymentStatusChanged().getStatus().isSetCaptured());
    }

    @Override
    public PaymentRow map(InvoiceChange change, MachineEvent event) {
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(event.getSourceId(), findPayment(),
                paymentId, event.getEventId());

        PaymentRow paymentRow = paymentSinkRowFactory.create(event, invoicePaymentWrapper, paymentId);

        paymentRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(), PaymentStatus.class));

        if (invoicePaymentStatusChanged.getStatus().isSetFailed()) {
            OperationFailure operationFailure = invoicePaymentStatusChanged.getStatus().getFailed().getFailure();
            if (operationFailure.isSetFailure()) {
                Failure failure = operationFailure.getFailure();
                paymentRow.setErrorCode(TErrorUtil.toStringVal(failure));
                paymentRow.setErrorReason(failure.getReason());
            } else if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                paymentRow.setErrorCode(OPERATION_TIMEOUT);
            }
            if (paymentRow.getCashFlowResult() == null || paymentRow.getCashFlowResult().getAmount() == 0) {
                paymentRow.setCashFlowResult(CashFlowResult.builder()
                        .amount(invoicePaymentWrapper.getInvoicePayment().getPayment().getCost().getAmount())
                        .build());
            }
        }

        log.debug("PaymentMapper paymentRow: {}", paymentRow);
        return paymentRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_STATUS_CHANGED;
    }

}
