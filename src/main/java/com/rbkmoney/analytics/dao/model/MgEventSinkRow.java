package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@NoArgsConstructor
public class MgEventSinkRow {

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
    private String errorMessage;

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

    private MgEventSinkRow oldMgEventSinkRow;

    public MgEventSinkRow(MgEventSinkRow mgEventSinkRow) {
        this.timestamp = mgEventSinkRow.getTimestamp();
        this.eventTime = mgEventSinkRow.getEventTime();
        this.eventTimeHour = mgEventSinkRow.getEventTimeHour();

        this.ip = mgEventSinkRow.getIp();
        this.email = mgEventSinkRow.getEmail();
        this.bin = mgEventSinkRow.getBin();
        this.fingerprint = mgEventSinkRow.getFingerprint();

        this.shopId = mgEventSinkRow.getShopId();
        this.partyId = mgEventSinkRow.getPartyId();

        this.status = mgEventSinkRow.getStatus();
        this.errorCode = mgEventSinkRow.getErrorCode();
        this.errorMessage = mgEventSinkRow.getErrorMessage();

        this.amount = mgEventSinkRow.getAmount();
        this.currency = mgEventSinkRow.getCurrency();

        this.ipCountry = mgEventSinkRow.getIpCountry();
        this.bankCountry = mgEventSinkRow.getBankCountry();
        this.maskedPan = mgEventSinkRow.getMaskedPan();
        this.provider = mgEventSinkRow.getProvider();
        this.paymentTool = mgEventSinkRow.getPaymentTool();

        this.invoiceId = mgEventSinkRow.getInvoiceId();
        this.paymentId = mgEventSinkRow.getPaymentId();
        this.sequenceId = mgEventSinkRow.getSequenceId();

        this.sign = -1;

    }
}
