package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.computer.CashFlowComputer;
import dev.vality.analytics.constant.ChargebackCategory;
import dev.vality.analytics.constant.ChargebackStage;
import dev.vality.analytics.dao.model.ChargebackRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.exception.ChargebackInfoNotFoundException;
import dev.vality.analytics.service.GeoProvider;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.damsel.payment_processing.InvoicePaymentChargeback;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ChargebackRowFactory extends InvoiceBaseRowFactory<ChargebackRow> {

    private final CashFlowComputer cashFlowComputer;

    public ChargebackRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public ChargebackRow create(MachineEvent machineEvent,
                                InvoicePaymentWrapper invoicePaymentWrapper,
                                String chargebackId) {
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        return payment.getChargebacks().stream()
                .filter(chargeback -> chargeback.getChargeback().getId().equals(chargebackId))
                .findFirst()
                .map(chargeback -> mapRow(machineEvent, payment, invoice, chargebackId, chargeback))
                .orElseThrow(ChargebackInfoNotFoundException::new);
    }

    private ChargebackRow mapRow(MachineEvent machineEvent,
                                 InvoicePayment payment,
                                 Invoice invoice,
                                 String chargebackId,
                                 InvoicePaymentChargeback chargeback) {
        ChargebackRow row = new ChargebackRow();
        List<FinalCashFlowPosting> cashFlow = chargeback.isSetCashFlow()
                ? chargeback.getCashFlow() : payment.getCashFlow();
        row.setChargebackId(chargebackId);
        row.setPaymentId(payment.getPayment().getId());
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        var invoicePaymentChargeback = chargeback.getChargeback();
        row.setChargebackCode(invoicePaymentChargeback.getReason().getCode());
        row.setCategory(TBaseUtil.unionFieldToEnum(invoicePaymentChargeback.getReason().getCategory(),
                ChargebackCategory.class)
        );
        row.setStage(TBaseUtil.unionFieldToEnum(invoicePaymentChargeback.getStage(), ChargebackStage.class));
        initBaseRow(machineEvent, row, payment, invoice);

        return row;
    }

}
