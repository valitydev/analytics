package dev.vality.analytics.service.model;

import lombok.Data;

@Data
public class RateGroupingKey {

    private final String sourceId;
    private final String sourceCode;
    private final String destinationCode;

}
