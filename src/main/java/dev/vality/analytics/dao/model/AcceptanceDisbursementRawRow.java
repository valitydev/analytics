package dev.vality.analytics.dao.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AcceptanceDisbursementRawRow {

    private LocalDate date;
    private String currency;
    private String locationUrl;
    private long turnover;
    private long cost;

}
