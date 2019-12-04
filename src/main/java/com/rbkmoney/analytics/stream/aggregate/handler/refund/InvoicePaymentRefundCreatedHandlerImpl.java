package com.rbkmoney.analytics.stream.aggregate.handler.refund;

import com.rbkmoney.analytics.constant.RefundStatus;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.utils.JsonUtil;
import com.rbkmoney.analytics.utils.MgRefundUtils;
import com.rbkmoney.damsel.domain.InvoicePaymentRefund;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentRefundCreated;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentRefundCreatedHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InvoicePaymentRefundCreatedHandlerImpl extends InvoicePaymentRefundCreatedHandler<MgRefundRow> {

    @Override
    public MgRefundRow handle(InvoiceChange change, SinkEvent event) {
        log.debug("InvoicePaymentRefundCreatedHandlerImpl change: {} event: {}", change, event);

        MgRefundRow mgRefundRow = new MgRefundRow();

        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();

        InvoicePaymentRefundChange invoicePaymentRefundChange = invoicePaymentChange.getPayload()
                .getInvoicePaymentRefundChange();
        InvoicePaymentRefundCreated invoicePaymentRefundCreated = invoicePaymentRefundChange.getPayload()
                .getInvoicePaymentRefundCreated();

        InvoicePaymentRefund invoicePaymentRefund = invoicePaymentRefundCreated.getRefund();

        String refundId = invoicePaymentRefundChange.getId();

        MgRefundUtils.initTimeFields(mgRefundRow, invoicePaymentRefund.getCreatedAt());

        mgRefundRow.setSequenceId((event.getEvent().getEventId()));
        mgRefundRow.setInvoiceId(event.getEvent().getSourceId());
        mgRefundRow.setRefundId(refundId);

        String paymentId = invoicePaymentChange.getId();
        mgRefundRow.setPaymentId(paymentId);

        mgRefundRow.setStatus(TBaseUtil.unionFieldToEnum(invoicePaymentRefund.getStatus(), RefundStatus.class));
        if (invoicePaymentRefund.getStatus().isSetFailed()) {
            mgRefundRow.setErrorCode(JsonUtil.tBaseToJsonString(invoicePaymentRefund.getStatus().getFailed()));
        }

        if (invoicePaymentRefund.isSetCash()) {
            mgRefundRow.setAmount(invoicePaymentRefund.getCash().getAmount());
            mgRefundRow.setCurrency(invoicePaymentRefund.getCash().getCurrency().getSymbolicCode());
        }

        mgRefundRow.setReason(invoicePaymentRefund.getReason());

        return mgRefundRow;
    }


}
