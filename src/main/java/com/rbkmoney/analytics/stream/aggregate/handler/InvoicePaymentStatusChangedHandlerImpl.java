package com.rbkmoney.analytics.stream.aggregate.handler;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
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
public class InvoicePaymentStatusChangedHandlerImpl extends InvoicePaymentStatusChangedHandler<MgEventSinkRow> {

    public static final String OPERATION_TIMEOUT = "operation_timeout";

    @Override
    public MgEventSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgEventSinkRow mgEventSinkRow = new MgEventSinkRow();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        mgEventSinkRow.setPaymentId(invoicePaymentChange.getId());
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStatusChanged invoicePaymentStatusChanged = payload.getInvoicePaymentStatusChanged();
        mgEventSinkRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentStatusChanged.getStatus(), PaymentStatus.class));
        mgEventSinkRow.setSequenceId((event.getEvent().getEventId()));

        if (invoicePaymentStatusChanged.getStatus().isSetFailed()) {
            if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetFailure()) {
                Failure failure = invoicePaymentStatusChanged.getStatus().getFailed().getFailure().getFailure();
                mgEventSinkRow.setErrorMessage(failure.getReason());
                mgEventSinkRow.setErrorCode(failure.getCode());
            } else if (invoicePaymentStatusChanged.getStatus().getFailed().getFailure().isSetOperationTimeout()) {
                mgEventSinkRow.setErrorCode(OPERATION_TIMEOUT);
            }
        }

        return mgEventSinkRow;
    }

}
