package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.RefundStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@NoArgsConstructor
public class MgRefundRow {

    private Date timestamp;
    private Long eventTime;
    private Long eventTimeHour;

    private String ip;
    private String email;
    private String fingerprint;

    private RefundStatus status;
    private String errorCode;

    private Long amount;
    private String currency;

    private String shopId;
    private String partyId;

    private String provider;
    private String reason;

    private String invoiceId;
    private String refundId;
    private String paymentId;
    private Long sequenceId;

    private short sign = 1;

    private MgRefundRow oldMgRefundRow;

    public MgRefundRow(MgRefundRow mgRefundRow) {
        this.timestamp = mgRefundRow.getTimestamp();
        this.eventTime = mgRefundRow.getEventTime();
        this.eventTimeHour = mgRefundRow.getEventTimeHour();

        this.ip = mgRefundRow.getIp();
        this.email = mgRefundRow.getEmail();
        this.fingerprint = mgRefundRow.getFingerprint();

        this.shopId = mgRefundRow.getShopId();
        this.partyId = mgRefundRow.getPartyId();

        this.status = mgRefundRow.getStatus();
        this.errorCode = mgRefundRow.getErrorCode();

        this.amount = mgRefundRow.getAmount();
        this.currency = mgRefundRow.getCurrency();

        this.provider = mgRefundRow.getProvider();

        this.invoiceId = mgRefundRow.getInvoiceId();
        this.refundId = mgRefundRow.getRefundId();
        this.paymentId = mgRefundRow.getPaymentId();
        this.sequenceId = mgRefundRow.getSequenceId();

        this.sign = -1;
    }
}