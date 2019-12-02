package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@NoArgsConstructor
public class MgPaymentSinkRow {

    private Date timestamp;
    private Long eventTime;
    private Long eventTimeHour;

    private String ip;
    private String email;
    private String bin;
    private String fingerprint;

    private String shopId;
    private String partyId;

    private PaymentStatus status;
    private String errorCode;

    private Long amount;
    private String currency;

    private String ipCountry;
    private String bankCountry;
    private String maskedPan;
    private String provider;
    private PaymentToolType paymentTool;

    private String invoiceId;
    private String paymentId;
    private Long sequenceId;

    private short sign = 1;

    private MgPaymentSinkRow oldMgPaymentSinkRow;

    public MgPaymentSinkRow(MgPaymentSinkRow mgPaymentSinkRow) {
        this.timestamp = mgPaymentSinkRow.getTimestamp();
        this.eventTime = mgPaymentSinkRow.getEventTime();
        this.eventTimeHour = mgPaymentSinkRow.getEventTimeHour();

        this.ip = mgPaymentSinkRow.getIp();
        this.email = mgPaymentSinkRow.getEmail();
        this.bin = mgPaymentSinkRow.getBin();
        this.fingerprint = mgPaymentSinkRow.getFingerprint();

        this.shopId = mgPaymentSinkRow.getShopId();
        this.partyId = mgPaymentSinkRow.getPartyId();

        this.status = mgPaymentSinkRow.getStatus();
        this.errorCode = mgPaymentSinkRow.getErrorCode();

        this.amount = mgPaymentSinkRow.getAmount();
        this.currency = mgPaymentSinkRow.getCurrency();

        this.ipCountry = mgPaymentSinkRow.getIpCountry();
        this.bankCountry = mgPaymentSinkRow.getBankCountry();
        this.maskedPan = mgPaymentSinkRow.getMaskedPan();
        this.provider = mgPaymentSinkRow.getProvider();
        this.paymentTool = mgPaymentSinkRow.getPaymentTool();

        this.invoiceId = mgPaymentSinkRow.getInvoiceId();
        this.paymentId = mgPaymentSinkRow.getPaymentId();
        this.sequenceId = mgPaymentSinkRow.getSequenceId();

        this.sign = -1;

    }
}
