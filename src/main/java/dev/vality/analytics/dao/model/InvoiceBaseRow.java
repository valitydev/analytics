package dev.vality.analytics.dao.model;

import dev.vality.analytics.domain.CashFlowResult;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class InvoiceBaseRow {

    private LocalDateTime eventTime;
    private LocalDateTime paymentTime;

    private String ip;
    private String email;
    private String phoneNumber;
    private String fingerprint;
    private String cardToken;
    private String cardHolderName;
    private String paymentSystem;
    private String digitalWalletProvider;
    private String digitalWalletToken;
    private String cryptoCurrency;
    private String mobileOperator;

    private CashFlowResult cashFlowResult;
    private String currency;

    private String shopId;
    private String partyId;

    private String provider;
    private Integer providerId;
    private Integer terminal;

    private String invoiceId;
    private String paymentId;

    private Long sequenceId;

    private String paymentCountry;
    private String bankCountry;

    private String paymentTool;
    private String bankCardTokenProvider;
    private String bin;

    private String maskedPan;

    private String paymentTerminal;

    private String rrn;

    private String riskScore;

    private String errorCode;
    private String errorReason;

}
