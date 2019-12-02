package com.rbkmoney.analytics.stream.aggregate.handler.refund;

import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.utils.JsonUtil;
import com.rbkmoney.analytics.utils.MgRefundUtils;
import com.rbkmoney.damsel.domain.InvoicePaymentRefundStatus;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentRefundStatusChangedHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InvoicePaymentRefundStatusChangedHandlerImpl extends InvoicePaymentRefundStatusChangedHandler<MgRefundRow> {

    @Override
    public MgRefundRow handle(InvoiceChange change, SinkEvent event) {
        MgRefundRow mgRefundRow = new MgRefundRow();

        String invoiceId = event.getEvent().getSourceId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentRefundChange invoicePaymentRefundChange = invoicePaymentChange.getPayload()
                .getInvoicePaymentRefundChange();
        InvoicePaymentRefundStatus invoicePaymentRefundStatus = invoicePaymentRefundChange.getPayload().getInvoicePaymentRefundStatusChanged().getStatus();
        String refundId = invoicePaymentRefundChange.getId();

        MgRefundUtils.initTimeFields(mgRefundRow, event.getEvent().getCreatedAt());

        mgRefundRow.setInvoiceId(invoiceId);
        mgRefundRow.setPaymentId(paymentId);
        mgRefundRow.setRefundId(refundId);

        mgRefundRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentRefundStatus, RefundStatus.class));
        if (invoicePaymentRefundStatus.isSetFailed()) {
            mgRefundRow.setErrorCode(JsonUtil.tBaseToJsonString(invoicePaymentRefundStatus.getFailed()));
        } else {
            mgRefundRow.setErrorCode(null);
        }

        return mgRefundRow;
    }
}
