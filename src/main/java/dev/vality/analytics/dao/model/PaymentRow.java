package dev.vality.analytics.dao.model;

import dev.vality.analytics.constant.PaymentStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class PaymentRow extends InvoiceBaseRow {

    private PaymentStatus status;

}
