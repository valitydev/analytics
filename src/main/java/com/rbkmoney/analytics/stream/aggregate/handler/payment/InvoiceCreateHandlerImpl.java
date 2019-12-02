package com.rbkmoney.analytics.stream.aggregate.handler.payment;

import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoiceCreated;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoiceCreateHandler;
import org.springframework.stereotype.Component;

@Component
public class InvoiceCreateHandlerImpl extends InvoiceCreateHandler<MgPaymentSinkRow> {

    @Override
    public MgPaymentSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        InvoiceCreated invoiceCreated = change.getInvoiceCreated();
        Invoice invoice = invoiceCreated.getInvoice();
        mgPaymentSinkRow.setInvoiceId(invoice.getId());
        mgPaymentSinkRow.setShopId(invoice.getShopId());
        Cash cost = invoice.getCost();
        mgPaymentSinkRow.setAmount(cost.getAmount());
        mgPaymentSinkRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgPaymentSinkRow.setPartyId((invoice.getOwnerId()));
        mgPaymentSinkRow.setSequenceId((event.getEvent().getEventId()));
        return mgPaymentSinkRow;
    }

}
