package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.computer.CashFlowComputer;
import dev.vality.analytics.computer.ReversedCashFlowComputer;
import dev.vality.analytics.dao.model.AdjustmentRow;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.exception.AdjustmentInfoNotFoundException;
import dev.vality.analytics.service.GeoProvider;
import dev.vality.damsel.domain.FinalCashFlowPosting;
import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.domain.InvoicePaymentAdjustment;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class AdjustmentRowFactory extends InvoiceBaseRowFactory<AdjustmentRow> {

    private final CashFlowComputer cashFlowComputer;
    private final ReversedCashFlowComputer reversedCashFlowComputer;

    public AdjustmentRowFactory(GeoProvider geoProvider,
                                CashFlowComputer cashFlowComputer,
                                ReversedCashFlowComputer reversedCashFlowComputer) {
        super(geoProvider);
        this.cashFlowComputer = cashFlowComputer;
        this.reversedCashFlowComputer = reversedCashFlowComputer;
    }

    @Override
    public AdjustmentRow create(MachineEvent machineEvent,
                                InvoicePaymentWrapper invoicePaymentWrapper,
                                String adjustmentId) {
        Invoice invoice = invoicePaymentWrapper.getInvoice();
        InvoicePayment payment = invoicePaymentWrapper.getInvoicePayment();
        return payment.getAdjustments().stream()
                .filter(adjustment -> adjustment.getId().equals(adjustmentId))
                .findFirst()
                .map(adjustment -> mapRow(machineEvent, payment, invoice, adjustmentId, adjustment))
                .orElseThrow(AdjustmentInfoNotFoundException::new);
    }

    private AdjustmentRow mapRow(MachineEvent machineEvent,
                                 InvoicePayment payment,
                                 Invoice invoice,
                                 String id,
                                 InvoicePaymentAdjustment adjustment) {
        AdjustmentRow row = new AdjustmentRow();
        row.setAdjustmentId(id);
        row.setPaymentId(payment.getPayment().getId());
        initBaseRow(machineEvent, row, payment, invoice);

        List<FinalCashFlowPosting> cashFlow = adjustment.getNewCashFlow();
        row.setCashFlowResult(cashFlowComputer.compute(cashFlow));

        List<FinalCashFlowPosting> oldCashFlow = adjustment.getOldCashFlowInverse();
        row.setOldCashFlowResult(reversedCashFlowComputer.compute(oldCashFlow));
        return row;
    }

}
