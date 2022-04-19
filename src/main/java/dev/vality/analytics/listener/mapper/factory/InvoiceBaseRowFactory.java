package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.constant.ClickHouseUtilsValue;
import dev.vality.analytics.dao.model.InvoiceBaseRow;
import dev.vality.analytics.service.GeoProvider;
import dev.vality.damsel.domain.*;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public abstract class InvoiceBaseRowFactory<T extends InvoiceBaseRow> implements RowFactory<T> {

    private final GeoProvider geoProvider;

    @Override
    public void initBaseRow(MachineEvent machineEvent, T row, InvoicePayment invoicePayment, Invoice invoice) {
        row.setEventTime(TypeUtil.stringToLocalDateTime(machineEvent.getCreatedAt()));
        var payment = invoicePayment.getPayment();

        row.setPartyId(invoice.getOwnerId());
        row.setShopId(invoice.getShopId());
        row.setInvoiceId(machineEvent.getSourceId());
        row.setPaymentId(payment.getId());
        row.setSequenceId((machineEvent.getEventId()));

        row.setCurrency(payment.getCost().getCurrency().getSymbolicCode());
        row.setPaymentTime(TypeUtil.stringToLocalDateTime(payment.getCreatedAt()));
        if (invoicePayment.isSetRoute()) {
            row.setTerminal(invoicePayment.getRoute().getTerminal().getId());
            row.setProviderId(invoicePayment.getRoute().getProvider().getId());
        }
        Payer payer = payment.getPayer();
        if (payer.isSetPaymentResource() && payer.getPaymentResource().isSetResource()) {
            DisposablePaymentResource resource = payer.getPaymentResource().getResource();
            PaymentTool paymentTool = resource.getPaymentTool();
            if (resource.isSetClientInfo()) {
                ClientInfo clientInfo = resource.getClientInfo();
                String ipAddress = clientInfo.getIpAddress();
                row.setIp(ipAddress);
                row.setPaymentCountry(geoProvider.getLocationIsoCode(ipAddress));
                row.setFingerprint(clientInfo.getFingerprint());
            }
            initCardData(row, paymentTool);
            initContactInfo(row, payer.getPaymentResource().getContactInfo());
        } else if (payer.isSetCustomer()) {
            CustomerPayer customer = payer.getCustomer();
            PaymentTool paymentTool = customer.getPaymentTool();
            initContactInfo(row, customer.getContactInfo());
            initCardData(row, paymentTool);
        } else if (payer.isSetRecurrent()) {
            RecurrentPayer recurrent = payer.getRecurrent();
            PaymentTool paymentTool = recurrent.getPaymentTool();
            initCardData(row, paymentTool);
            initContactInfo(row, recurrent.getContactInfo());
        } else {
            log.warn("Unknown payment tool in payer: {}", payer);
        }
    }

    private void initContactInfo(T row, ContactInfo contactInfo) {
        if (contactInfo != null) {
            row.setEmail(contactInfo.getEmail());
            row.setPhoneNumber(contactInfo.getPhoneNumber());
        }
    }

    private void initCardData(T row, PaymentTool paymentTool) {
        row.setPaymentTool(paymentTool.getSetField().getFieldName());
        if (paymentTool.isSetBankCard()) {
            BankCard bankCard = paymentTool.getBankCard();
            row.setBankCountry(bankCard.isSetIssuerCountry()
                    ? bankCard.getIssuerCountry().name() : ClickHouseUtilsValue.UNKNOWN);
            row.setProvider(bankCard.getBankName());
            row.setCardToken(bankCard.getToken());
            row.setCardHolderName(StringUtils.hasLength(bankCard.getCardholderName())
                    ? bankCard.getCardholderName() : ClickHouseUtilsValue.UNKNOWN);
            row.setBin(bankCard.getBin());
            row.setMaskedPan(bankCard.getLastDigits());
            row.setPaymentSystem(
                    Optional.ofNullable(bankCard.getPaymentSystem()).map(PaymentSystemRef::getId).orElse(null)
            );
            row.setBankCardTokenProvider(
                    Optional.ofNullable(bankCard.getPaymentToken()).map(BankCardTokenServiceRef::getId).orElse(null)
            );
        } else if (paymentTool.isSetPaymentTerminal()) {
            row.setPaymentTerminal(
                    Optional.ofNullable(paymentTool.getPaymentTerminal().getPaymentService())
                            .map(PaymentServiceRef::getId).orElse(null)
            );
        } else if (paymentTool.isSetDigitalWallet()) {
            row.setDigitalWalletProvider(
                    Optional.ofNullable(paymentTool.getDigitalWallet().getPaymentService())
                            .map(PaymentServiceRef::getId).orElse(null));
            row.setDigitalWalletToken(paymentTool.getDigitalWallet().getToken());
        } else if (paymentTool.isSetCryptoCurrency()) {
            row.setCryptoCurrency(paymentTool.getCryptoCurrency().getId());
        } else if (paymentTool.isSetMobileCommerce()) {
            row.setMobileOperator(
                    Optional.ofNullable(paymentTool.getMobileCommerce().getOperator())
                            .map(MobileOperatorRef::getId).orElse(null));
        }
    }
}
