package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.dao.model.InvoiceBaseRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface RowFactory<T extends InvoiceBaseRow> {

    T create(MachineEvent machineEvent, InvoicePaymentWrapper invoiceInfo, String id);

    void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment, Invoice invoice);

}
