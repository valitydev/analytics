package com.rbkmoney.analytics.dao.model;

import lombok.Data;

@Data
public class SplitCost extends Cost {

    private Long offset;
    private Long amount;
    private String currency;

}
