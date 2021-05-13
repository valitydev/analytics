package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.damsel.domain.PaymentTool;
import com.rbkmoney.geck.common.util.TBaseUtil;

import java.util.Objects;

public class PaymentToolTypeUtil {

    private PaymentToolTypeUtil() {
    }

    public static PaymentToolType getPaymentToolType(PaymentTool paymentTool) {
        Objects.requireNonNull(paymentTool);
        if (paymentTool.isSetCryptoCurrencyDeprecated()) {
            return PaymentToolType.crypto_currency;
        }
        return TBaseUtil.unionFieldToEnum(paymentTool, PaymentToolType.class);
    }

}
