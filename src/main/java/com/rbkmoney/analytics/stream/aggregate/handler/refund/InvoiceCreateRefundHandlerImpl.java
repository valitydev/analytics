package com.rbkmoney.analytics.stream.aggregate.handler.refund;

import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoiceCreated;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoiceCreateHandler;
import org.springframework.stereotype.Component;

@Component
public class InvoiceCreateRefundHandlerImpl extends InvoiceCreateHandler<MgRefundRow> {

    @Override
    public MgRefundRow handle(InvoiceChange change, SinkEvent event) {
        MgRefundRow mgRefundRow = new MgRefundRow();
        InvoiceCreated invoiceCreated = change.getInvoiceCreated();
        Invoice invoice = invoiceCreated.getInvoice();
        mgRefundRow.setInvoiceId(invoice.getId());
        mgRefundRow.setShopId(invoice.getShopId());
        Cash cost = invoice.getCost();
        mgRefundRow.setAmount(cost.getAmount());
        mgRefundRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgRefundRow.setPartyId((invoice.getOwnerId()));
        mgRefundRow.setSequenceId((event.getEvent().getEventId()));
        return mgRefundRow;
    }

}
