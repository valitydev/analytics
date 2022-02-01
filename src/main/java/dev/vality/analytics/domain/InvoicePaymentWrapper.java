package dev.vality.analytics.domain;

import dev.vality.damsel.domain.Invoice;
import dev.vality.damsel.payment_processing.InvoicePayment;
import lombok.Data;

@Data
public class InvoicePaymentWrapper {

    private Invoice invoice;
    private InvoicePayment invoicePayment;

}
