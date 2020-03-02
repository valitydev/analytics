package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.RefundStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class MgRefundRow extends MgBaseRow {

    private RefundStatus status;
    private String errorCode;

    private String provider;
    private String reason;
    private String refundId;

}