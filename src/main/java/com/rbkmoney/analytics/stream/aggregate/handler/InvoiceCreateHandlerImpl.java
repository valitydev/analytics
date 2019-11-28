package com.rbkmoney.analytics.stream.aggregate.handler;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.damsel.domain.Cash;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoiceCreated;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoiceCreateHandler;
import org.springframework.stereotype.Component;

@Component
public class InvoiceCreateHandlerImpl extends InvoiceCreateHandler<MgEventSinkRow> {

    @Override
    public MgEventSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgEventSinkRow mgEventSinkRow = new MgEventSinkRow();
        InvoiceCreated invoiceCreated = change.getInvoiceCreated();
        Invoice invoice = invoiceCreated.getInvoice();
        mgEventSinkRow.setInvoiceId(invoice.getId());
        mgEventSinkRow.setShopId(invoice.getShopId());
        Cash cost = invoice.getCost();
        mgEventSinkRow.setAmount(cost.getAmount());
        mgEventSinkRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgEventSinkRow.setPartyId((invoice.getOwnerId()));
        mgEventSinkRow.setSequenceId((event.getEvent().getEventId()));
        return mgEventSinkRow;
    }

}
