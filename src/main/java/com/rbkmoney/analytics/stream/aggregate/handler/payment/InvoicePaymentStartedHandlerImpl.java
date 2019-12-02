package com.rbkmoney.analytics.stream.aggregate.handler.payment;

import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
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
public class InvoicePaymentStartedHandlerImpl extends InvoicePaymentStartedHandler<MgPaymentSinkRow> {

    @Override
    public MgPaymentSinkRow handle(InvoiceChange change, SinkEvent event) {
        MgPaymentSinkRow mgPaymentSinkRow = new MgPaymentSinkRow();
        InvoicePaymentChange invoicePaymentChange = change.getInvoicePaymentChange();
        mgPaymentSinkRow.setPaymentId(invoicePaymentChange.getId());
        InvoicePaymentChangePayload payload = invoicePaymentChange.getPayload();
        InvoicePaymentStarted invoicePaymentStarted = payload.getInvoicePaymentStarted();
        Payer payer = invoicePaymentStarted.getPayment().getPayer();
        InvoicePayment payment = invoicePaymentStarted.getPayment();
        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(payment.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgPaymentSinkRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgPaymentSinkRow.setEventTime(timestamp);
        long eventTimeHour = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.HOURS).toEpochMilli();
        mgPaymentSinkRow.setEventTimeHour(eventTimeHour);

        Cash cost = payment.getCost();
        mgPaymentSinkRow.setSequenceId((event.getEvent().getEventId()));
        mgPaymentSinkRow.setAmount(cost.getAmount());
        mgPaymentSinkRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgPaymentSinkRow.setStatus(TBaseUtil.unionFieldToEnum(payment.getStatus(), PaymentStatus.class));
        if (payer.isSetPaymentResource()) {
            mgPaymentSinkRow.setPaymentTool(TBaseUtil.unionFieldToEnum(payer.getPaymentResource().getResource().getPaymentTool(), PaymentToolType.class));
            if (payer.getPaymentResource().isSetResource()) {
                if (payer.getPaymentResource().getResource().isSetClientInfo()) {
                    ClientInfo clientInfo = payer.getPaymentResource().getResource().getClientInfo();
                    mgPaymentSinkRow.setIp(clientInfo.getIpAddress());
                    mgPaymentSinkRow.setFingerprint(clientInfo.getFingerprint());
                }
                if (isBankCard(payer)) {
                    BankCard bankCard = payer.getPaymentResource().getResource().getPaymentTool().getBankCard();
                    mgPaymentSinkRow.setBankCountry(bankCard.getIssuerCountry() != null ? bankCard.getIssuerCountry().name() : ClickhouseUtilsValue.UNKNOWN);
                    mgPaymentSinkRow.setBin(bankCard.getBin());
                    mgPaymentSinkRow.setMaskedPan(bankCard.getMaskedPan());
                    mgPaymentSinkRow.setProvider(bankCard.getBankName());
                }
            }
            mgPaymentSinkRow.setEmail(payer.getPaymentResource().getContactInfo().getEmail());
        }
        return mgPaymentSinkRow;
    }

    private boolean isBankCard(Payer payer) {
        return payer.getPaymentResource().isSetResource()
                && payer.getPaymentResource().getResource().isSetPaymentTool()
                && payer.getPaymentResource().getResource().getPaymentTool().isSetBankCard();
    }
}
