package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.constant.AdjustmentStatus;
import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.AdjustmentRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.listener.mapper.Mapper;
import dev.vality.analytics.listener.mapper.factory.RowFactory;
import dev.vality.analytics.service.HgClientService;
import dev.vality.damsel.payment_processing.*;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

@Slf4j
@Component
@RequiredArgsConstructor
public class AdjustmentMapper implements Mapper<InvoiceChange, MachineEvent, AdjustmentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<AdjustmentRow> adjustmentRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change);
    }

    @Override
    public AdjustmentRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentAdjustmentChange invoicePaymentAdjustmentChange = invoicePaymentChange
                .getPayload()
                .getInvoicePaymentAdjustmentChange();
        InvoicePaymentAdjustmentChangePayload payload = invoicePaymentAdjustmentChange.getPayload();
        String adjustmentChangeId = invoicePaymentAdjustmentChange.getId();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(), findPayment(), paymentId, adjustmentChangeId, event.getEventId());
        AdjustmentRow adjustmentRow = adjustmentRowFactory.create(event, invoicePaymentWrapper, adjustmentChangeId);

        adjustmentRow.setStatus(TBaseUtil.unionFieldToEnum(payload
                .getInvoicePaymentAdjustmentStatusChanged()
                .getStatus(), AdjustmentStatus.class));

        log.debug("AdjustmentMapper adjustmentRow: {}", adjustmentRow);
        return adjustmentRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment ->
                        payment.isSetPayment()
                                && payment.isSetAdjustments()
                                && payment.getAdjustments().stream()
                                .anyMatch(adjustment -> adjustment.getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_ADJUSTMENT_STATUS_CHANGED;
    }

}
