package dev.vality.analytics.listener.handler.invoice;

import dev.vality.analytics.listener.handler.BatchHandler;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface InvoiceBatchHandler extends BatchHandler<InvoiceChange, MachineEvent> {
}
