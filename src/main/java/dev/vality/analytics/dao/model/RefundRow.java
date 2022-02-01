package dev.vality.analytics.dao.model;

import dev.vality.analytics.constant.RefundStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class RefundRow extends InvoiceBaseRow {

    private RefundStatus status;

    private String reason;
    private String refundId;

}