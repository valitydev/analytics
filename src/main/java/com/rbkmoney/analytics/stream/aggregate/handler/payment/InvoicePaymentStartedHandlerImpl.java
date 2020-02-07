package com.rbkmoney.analytics.stream.aggregate.handler.payment;

import com.rbkmoney.analytics.constant.ClickhouseUtilsValue;
import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.utils.TimeUtils;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.payment_processing.InvoiceChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChange;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentChangePayload;
import com.rbkmoney.damsel.payment_processing.InvoicePaymentStarted;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.handler.flow.InvoicePaymentStartedHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static java.time.ZoneOffset.UTC;

@Slf4j
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
        if (payment.getStatus().isSetRefunded()) {
            return null;
        }

        LocalDateTime localDateTime = TypeUtil.stringToLocalDateTime(payment.getCreatedAt());
        long timestamp = localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        mgPaymentSinkRow.setTimestamp(java.sql.Date.valueOf(
                Instant.ofEpochMilli(timestamp)
                        .atZone(UTC)
                        .toLocalDate())
        );
        mgPaymentSinkRow.setEventTime(timestamp);
        long eventTimeHour = TimeUtils.parseEventTimeHour(timestamp);
        mgPaymentSinkRow.setEventTimeHour(eventTimeHour);

        Cash cost = payment.getCost();

        mgPaymentSinkRow.setPartyId(payment.getOwnerId());
        mgPaymentSinkRow.setShopId(payment.getShopId());

        mgPaymentSinkRow.setInvoiceId(event.getEvent().getSourceId());
        mgPaymentSinkRow.setSequenceId((event.getEvent().getEventId()));
        mgPaymentSinkRow.setAmount(cost.getAmount());
        mgPaymentSinkRow.setCurrency(cost.getCurrency().getSymbolicCode());
        mgPaymentSinkRow.setStatus(TBaseUtil.unionFieldToEnum(payment.getStatus(), PaymentStatus.class));

        if (payer.isSetPaymentResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            initPaymentTool(mgPaymentSinkRow, paymentTool);
            if (payer.getPaymentResource().isSetResource()) {
                if (resource.isSetClientInfo()) {
                    ClientInfo clientInfo = resource.getClientInfo();
                    mgPaymentSinkRow.setIp(clientInfo.getIpAddress());
                    mgPaymentSinkRow.setFingerprint(clientInfo.getFingerprint());
                }
                initCardData(mgPaymentSinkRow, paymentTool);
            }
            initContactInfo(mgPaymentSinkRow, payer.getPaymentResource().getContactInfo());
        } else if (payer.isSetCustomer()) {
            CustomerPayer customer = payer.getCustomer();
            PaymentTool paymentTool = customer.getPaymentTool();
            initPaymentTool(mgPaymentSinkRow, paymentTool);
            initContactInfo(mgPaymentSinkRow, customer.getContactInfo());
            initCardData(mgPaymentSinkRow, paymentTool);
        } else if (payer.isSetRecurrent()) {
            RecurrentPayer recurrent = payer.getRecurrent();
            PaymentTool paymentTool = recurrent.getPaymentTool();
            initPaymentTool(mgPaymentSinkRow, paymentTool);
            initCardData(mgPaymentSinkRow, paymentTool);
            initContactInfo(mgPaymentSinkRow, recurrent.getContactInfo());
        } else {
            log.warn("Unkonwn payment tool in payer: {}", payer);
        }
        return mgPaymentSinkRow;
    }

    private void initContactInfo(MgPaymentSinkRow mgPaymentSinkRow, ContactInfo contactInfo) {
        if (contactInfo != null) {
            mgPaymentSinkRow.setEmail(contactInfo.getEmail());
        }
    }

    private void initPaymentTool(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        mgPaymentSinkRow.setPaymentTool(TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class));
    }

    private void initCardData(MgPaymentSinkRow mgPaymentSinkRow, PaymentTool paymentTool) {
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            mgPaymentSinkRow.setBankCountry(bankCard.getIssuerCountry() != null ? bankCard.getIssuerCountry().name() : ClickhouseUtilsValue.UNKNOWN);
            mgPaymentSinkRow.setBin(bankCard.getBin());
            mgPaymentSinkRow.setMaskedPan(bankCard.getMaskedPan());
            mgPaymentSinkRow.setProvider(bankCard.getBankName());
        }
    }

}
