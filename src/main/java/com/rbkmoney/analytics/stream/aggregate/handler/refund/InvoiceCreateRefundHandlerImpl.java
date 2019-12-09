package com.rbkmoney.analytics.stream.aggregate.handler.refund;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.damsel.domain.InvoicePayment;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStarted;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentStartedHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InvoiceCreateRefundHandlerImpl extends InvoicePaymentStartedHandler<MgRefundRow> {

    @Override
    public MgRefundRow handle(InvoiceChange change, SinkEvent event) {
        log.debug("InvoiceCreateRefundHandlerImpl change: {} event: {}", change, event);
        MgRefundRow mgRefundRow = new MgRefundRow();

        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();

        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStarted invoicePaymentStarted = payload.getInvoicePaymentStarted();

        mgRefundRow.setInvoiceId(event.getEvent().getSourceId());

        InvoicePayment payment = invoicePaymentStarted.getPayment();
        mgRefundRow.setPaymentId(payment.getId());

        mgRefundRow.setShopId(payment.getShopId());
        mgRefundRow.setPartyId((payment.getOwnerId()));

        mgRefundRow.setSequenceId((event.getEvent().getEventId()));

        if (payment.isSetCost()) {
            mgRefundRow.setAmount(payment.getCost().getAmount());
            mgRefundRow.setCurrency(payment.getCost().getCurrency().getSymbolicCode());
        }

        return mgRefundRow;
    }

}
