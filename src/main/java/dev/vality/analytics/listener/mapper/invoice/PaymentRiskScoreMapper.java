package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.listener.mapper.AbstractMapper;
import dev.vality.analytics.listener.mapper.factory.RowFactory;
import dev.vality.analytics.service.HgClientService;
import dev.vality.damsel.domain.RiskScore;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.damsel.payment_processing.InvoicePaymentChange;
import dev.vality.damsel.payment_processing.InvoicePaymentRiskScoreChanged;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRiskScoreMapper extends AbstractMapper<InvoiceChange, MachineEvent, PaymentRow> {

    private final HgClientService hgClientService;
    private final RowFactory<PaymentRow> paymentSinkRowFactory;

    @Override
    public PaymentRow map(InvoiceChange change, MachineEvent event) {
        String paymentId = change.getInvoicePaymentChange().getId();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        InvoicePaymentRiskScoreChanged invoicePaymentRiskScoreChanged = invoicePaymentChange.getPayload()
                .getInvoicePaymentRiskScoreChanged();
        RiskScore riskScore = invoicePaymentRiskScoreChanged.getRiskScore();

        InvoicePaymentWrapper invoicePaymentWrapper = hgClientService.getInvoiceInfo(
                event.getSourceId(),
                findPayment(),
                paymentId, event.getEventId());
        PaymentRow paymentRow = paymentSinkRowFactory.create(event, invoicePaymentWrapper, paymentId);

        paymentRow.setRiskScore(riskScore.name());

        log.debug("PaymentRiskScoreMapper paymentRow: {}", paymentRow);

        return paymentRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.INVOICE_PAYMENT_RISK_SCORE_CHANGED;
    }

}
