package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.computer.CashFlowComputer;
import dev.vality.analytics.dao.model.RefundRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.exception.AdjustmentInfoNotFoundException;
import dev.vality.analytics.service.GeoProvider;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.damsel.payment_processing.InvoicePaymentRefund;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class RefundRowFactory extends InvoiceBaseRowFactory<RefundRow> {

    private final CashFlowComputer cashFlowComputer;

    public RefundRowFactory(GeoProvider geoProvider, CashFlowComputer cashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
    }

    @Override
    public RefundRow create(MachineEvent machineEvent, InvoicePaymentWrapper invoicePaymentWrapper, String refundId) {
        InvoicePayment invoicePayment = invoicePaymentWrapper.getInvoicePayment();
        return invoicePayment.getRefunds().stream()
                .filter(refund -> refund.getRefund().getId().equals(refundId))
                .findFirst()
                .map(refund -> mapRow(machineEvent, invoicePayment, invoicePaymentWrapper.getInvoice(), refundId,
                        refund))
                .orElseThrow(AdjustmentInfoNotFoundException::new);
    }

    private RefundRow mapRow(MachineEvent machineEvent, InvoicePayment payment, Invoice invoice,
                             String refundId, InvoicePaymentRefund refund) {
        RefundRow row = new RefundRow();
        List<FinalCashFlowPosting> cashFlow = refund.isSetCashFlow() ? refund.getCashFlow() : payment.getCashFlow();
        row.setRefundId(refundId);
        row.setPaymentId(payment.getPayment().getId());
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));
        initBaseRow(machineEvent, row, payment, invoice);
        return row;
    }

}
