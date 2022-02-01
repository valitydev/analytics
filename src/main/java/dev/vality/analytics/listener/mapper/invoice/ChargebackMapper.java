package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.constant.ChargebackStatus;
import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.ChargebackRow;
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
public class ChargebackMapper implements Mapper<InvoiceChange, MachineEvent, ChargebackRow> {

    private final HgClientService hgClientService;
    private final RowFactory<ChargebackRow> chargebackRowRowFactory;

    @Override
    public boolean accept(InvoiceChange change) {
        return getChangeType().getFilter().match(change)
                && (change.getInvoicePaymentChange()
                .getPayload().getInvoicePaymentChargebackChange().getPayload()
                .getInvoicePaymentChargebackStatusChanged().getStatus().isSetAccepted()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentChargebackChange()
                .getPayload().getInvoicePaymentChargebackStatusChanged().getStatus().isSetCancelled()
                || change.getInvoicePaymentChange().getPayload().getInvoicePaymentChargebackChange()
                .getPayload().getInvoicePaymentChargebackStatusChanged().getStatus().isSetRejected());
    }

    @Override
    public ChargebackRow map(InvoiceChange change, MachineEvent event) {
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        String paymentId = invoicePaymentChange.getId();
        InvoicePaymentChargebackChange invoicePaymentChargebackChange =
                invoicePaymentChange.getPayload().getInvoicePaymentChargebackChange();
        InvoicePaymentChargebackChangePayload payload = invoicePaymentChargebackChange.getPayload();
        InvoicePaymentChargebackStatusChanged invoicePaymentChargebackStatusChanged =
                payload.getInvoicePaymentChargebackStatusChanged();

        String chargebackId = invoicePaymentChargebackChange.getId();
        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(event.getSourceId(), findPayment(),
                paymentId, chargebackId, event.getEventId());
        ChargebackRow chargebackRow = chargebackRowRowFactory.create(event, invoicePaymentWrapper, chargebackId);

        chargebackRow.setStatus(TBaseUtil.unionFieldToEnum(
                invoicePaymentChargebackStatusChanged.getStatus(), ChargebackStatus.class));

        log.debug("ChargebackPaymentMapper chargebackRow: {}", chargebackRow);
        return chargebackRow;
    }

    private BiFunction<String, Invoice, Optional<InvoicePayment>> findPayment() {
        return (id, invoiceInfo) -> invoiceInfo.getPayments().stream()
                .filter(payment -> payment.isSetPayment()
                        && payment.isSetChargebacks()
                        && payment.getChargebacks().stream()
                        .anyMatch(chargeback -> chargeback.getChargeback().getId().equals(id))
                )
                .findFirst();
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_CHARGEBACK_STATUS_CHANGED;
    }

}
