package com.rbkmoney.analytics.stream.aggregate.handler;

import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgEventSinkRow;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStarted;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentStartedHandler;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static java.time.ZoneOffset.UTC;

@Component
public class InvoicePaymentStartedHandlerImpl extends InvoicePaymentStartedHandler<MgEventSinkRow> {

    @Override
    public MgEventSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgEventSinkRow mgEventSinkRow = new MgEventSinkRow();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        mgEventSinkRow.setPaymentId(invoicePaymentChange.getId());
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStarted invoicePaymentStarted = payload.getInvoicePaymentStarted();
        Payer payer = invoicePaymentStarted.getPayment().getPayer();
        InvoicePayment payment = invoicePaymentStarted.getPayment();
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(payment.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgEventSinkRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgEventSinkRow.setEventTime(timestamp);
        long eventTimeHour = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.HOURS).toEpochMilli();
        mgEventSinkRow.setEventTimeHour(eventTimeHour);

        Cash cost = payment.getCost();
        mgEventSinkRow.setSequenceId((event.getEvent().getEventId()));
        mgEventSinkRow.setAmount(cost.getAmount());
        mgEventSinkRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgEventSinkRow.setStatus(TBaseUtil.unionFieldToEnum(payment.getStatus(), PaymentStatus.class));
        if (payer.isSetPaymentResource()) {
            mgEventSinkRow.setPaymentTool(TBaseUtil.unionFieldToEnum(payer.getPaymentResource().getResource().getPaymentTool(), PaymentToolType.class));
            if (payer.getPaymentResource().isSetResource()) {
                if (payer.getPaymentResource().getResource().isSetClientInfo()) {
                    ClientInfo clientInfo = payer.getPaymentResource().getResource().getClientInfo();
                    mgEventSinkRow.setIp(clientInfo.getIpAddress());
                    mgEventSinkRow.setFingerprint(clientInfo.getFingerprint());
                }
                if (isBankCard(payer)) {
                    BankCard bankCard = payer.getPaymentResource().getResource().getPaymentTool().getBankCard();
                    mgEventSinkRow.setBankCountry(bankCard.getIssuerCountry() != null ? bankCard.getIssuerCountry().name() : ClickhouseUtilsValue.UNKNOWN);
                    mgEventSinkRow.setBin(bankCard.getBin());
                    mgEventSinkRow.setMaskedPan(bankCard.getMaskedPan());
                    mgEventSinkRow.setCardToken(bankCard.getToken());
                    mgEventSinkRow.setBankName(bankCard.getBankName());
                }
            }
            mgEventSinkRow.setEmail(payer.getPaymentResource().getContactInfo().getEmail());
        }
        return mgEventSinkRow;
    }

    private boolean isBankCard(Payer payer) {
        return payer.getPaymentResource().isSetResource()
                && payer.getPaymentResource().getResource().isSetPaymentTool()
                && payer.getPaymentResource().getResource().getPaymentTool().isSetBankCard();
    }
}
