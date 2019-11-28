package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import lombok.Data;

import java.sql.Date;

@Data
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

    private String country;
    private Long amount;

    private String bankCountry;
    private String currency;
    private String maskedPan;
    private String bankName;
    private String cardToken;

    private String invoiceId;
    private String paymentId;
    private PaymentToolType paymentTool;

    private Long sequenceId;

    private Long oldSequenceId;

}
