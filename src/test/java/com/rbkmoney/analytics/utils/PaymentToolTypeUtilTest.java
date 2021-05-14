package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.damsel.domain.BankCard;
import com.rbkmoney.damsel.domain.CryptoCurrencyRef;
import com.rbkmoney.damsel.domain.DigitalWallet;
import com.rbkmoney.damsel.domain.LegacyCryptoCurrency;
import com.rbkmoney.damsel.domain.MobileCommerce;
import com.rbkmoney.damsel.domain.PaymentTerminal;
import com.rbkmoney.damsel.domain.PaymentTool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaymentToolTypeUtilTest {

    @Test
    void cryptoCurrencyTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setCryptoCurrency(new CryptoCurrencyRef("bitcoin"));
        assertEquals(PaymentToolType.crypto_currency, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }

    @Test
    void cryptoCurrencyDeprecatedTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setCryptoCurrencyDeprecated(LegacyCryptoCurrency.ethereum);
        assertEquals(PaymentToolType.crypto_currency, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }

    @Test
    void bankCardTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setBankCard(new BankCard());
        assertEquals(PaymentToolType.bank_card, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }

    @Test
    void paymentTerminalTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setPaymentTerminal(new PaymentTerminal());
        assertEquals(PaymentToolType.payment_terminal, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }

    @Test
    void digitalWalletTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setDigitalWallet(new DigitalWallet());
        assertEquals(PaymentToolType.digital_wallet, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }

    @Test
    void mobileCommerceTest() {
        PaymentTool paymentTool = new PaymentTool();
        paymentTool.setMobileCommerce(new MobileCommerce());
        assertEquals(PaymentToolType.mobile_commerce, PaymentToolTypeUtil.getPaymentToolType(paymentTool));
    }
}
