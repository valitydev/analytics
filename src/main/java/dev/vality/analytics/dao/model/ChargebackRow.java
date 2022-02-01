package dev.vality.analytics.dao.model;

import dev.vality.analytics.constant.ChargebackCategory;
import dev.vality.analytics.constant.ChargebackStage;
import dev.vality.analytics.constant.ChargebackStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class ChargebackRow extends InvoiceBaseRow {

    private ChargebackStatus status;
    private String chargebackId;

    private ChargebackCategory category;
    private String chargebackCode;
    private ChargebackStage stage;

}