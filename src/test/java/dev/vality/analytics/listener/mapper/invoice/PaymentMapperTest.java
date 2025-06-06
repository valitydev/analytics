package dev.vality.analytics.listener.mapper.invoice;

import dev.vality.analytics.dao.model.PaymentRow;
import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.domain.InvoicePaymentWrapper;
import dev.vality.analytics.listener.InvoiceFlowGenerator;
import dev.vality.analytics.listener.mapper.factory.PaymentRowFactory;
import dev.vality.analytics.service.HgClientService;
import dev.vality.analytics.utils.EventRangeFactory;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.CurrencyRef;
import dev.vality.damsel.payment_processing.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import dev.vality.damsel.payment_processing.InvoicingSrv;
import dev.vality.machinegun.eventsink.MachineEvent;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PaymentMapperTest {

    public static final long AMOUNT = 123L;

    @Mock
    private PaymentRowFactory paymentRowFactory;
    @Mock
    private InvoicingSrv.Iface invoicingClient;
    @Mock
    private EventRangeFactory eventRangeFactory;

    private PaymentMapper paymentMapper;

    @BeforeEach
    public void init() {
        HgClientService hgClientService = new HgClientService(invoicingClient, eventRangeFactory);
        paymentMapper = new PaymentMapper(hgClientService, paymentRowFactory);
    }

    @Test
    public void map() throws TException {
        String paymentId = "test";
        when(invoicingClient.get(any(), any())).thenReturn(createInvoice(paymentId));
        PaymentRow paymentRow = new PaymentRow();
        paymentRow.setCashFlowResult(CashFlowResult.EMPTY);
        when(paymentRowFactory.create(any(), any(), any())).thenReturn(paymentRow);
        PaymentRow sinkRow = paymentMapper.map(
                InvoiceFlowGenerator.createInvoiceFailed(paymentId), new MachineEvent());

        Assertions.assertEquals(AMOUNT, sinkRow.getCashFlowResult().getAmount());
    }

    private Invoice createInvoice(String paymentId) {
        InvoicePayment invoicePayment = createPaymentWrapper(paymentId);
        ArrayList<InvoicePayment> payments = new ArrayList<>();
        payments.add(invoicePayment);
        return new Invoice()
                .setInvoice(new dev.vality.damsel.domain.Invoice())
                .setPayments(payments);
    }

    private InvoicePayment createPaymentWrapper(String paymentId) {
        InvoicePaymentWrapper paymentWrapper = new InvoicePaymentWrapper();
        InvoicePayment invoicePayment = new InvoicePayment();
        invoicePayment.setPayment(new dev.vality.damsel.domain.InvoicePayment()
                .setId(paymentId)
                .setCost(new Cash()
                        .setAmount(AMOUNT)
                        .setCurrency(new CurrencyRef()
                                .setSymbolicCode("RUB"))));
        paymentWrapper.setInvoicePayment(invoicePayment);
        return invoicePayment;
    }
}
