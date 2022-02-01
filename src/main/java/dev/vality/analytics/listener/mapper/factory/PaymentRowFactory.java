package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.computer.CashFlowComputer;
import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.service.GeoProvider;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class PaymentRowFactory extends InvoiceBaseRowFactory<PaymentRow> {

    private final CashFlowComputer cashFlowComputer;

    public PaymentRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public PaymentRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String paymentId) {
        PaymentRow paymentRow = new PaymentRow();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        List<FinalCashFlowPosting> cashFlow = payment.getCashFlow();
        paymentRow.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, paymentRow, payment, invoicePaymentWrapper.getInvoice());
        return paymentRow;
    }

}
