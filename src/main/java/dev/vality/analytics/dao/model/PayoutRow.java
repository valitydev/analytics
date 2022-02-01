package dev.vality.analytics.dao.model;

import dev.vality.analytics.constant.PayoutStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class PayoutRow {

    private String payoutId;
    private PayoutStatus status;
    private String payoutToolId;
    private String statusCancelledDetails;
    private boolean isCancelledAfterBeingPaid;

    private LocalDateTime eventTime;
    private LocalDateTime payoutTime;

    private String shopId;
    private String partyId;

    private Long amount;
    private Long fee;
    private String currency;
}
