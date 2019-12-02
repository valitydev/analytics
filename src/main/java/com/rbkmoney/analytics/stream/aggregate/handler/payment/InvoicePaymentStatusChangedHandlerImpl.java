package com.rbkmoney.analytics.stream.aggregate.handler.payment;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.damsel.domain.Failure;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStatusChanged;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentStatusChangedHandler;
import org.springframework.stereotype.Component;

@Component
public class InvoicePaymentStatusChangedHandlerImpl extends InvoicePaymentStatusChangedHandler<MgPaymentSinkRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    @Override
    public MgPaymentSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        mgPaymentSinkRow.setPaymentId(invoicePaymentChange.getId());
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();
        mgPaymentSinkRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(), PaymentStatus.class));
        mgPaymentSinkRow.setSequenceId((event.getEvent().getEventId()));

        if (invoicePaymentStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentStatusChanged.getStatus().getFailed().getFailure().getFailure();
                mgPaymentSinkRow.setErrorCode(failure.getCode());
            } else if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                mgPaymentSinkRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        return mgPaymentSinkRow;
    }

}
