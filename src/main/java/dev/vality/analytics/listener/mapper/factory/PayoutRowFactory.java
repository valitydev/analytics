package dev.vality.analytics.listener.mapper.factory;

import dev.vality.analytics.dao.model.PayoutRow;
import dev.vality.geck.common.util.TBaseUtil;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.payout.manager.Event;
import dev.vality.payout.manager.Payout;
import dev.vality.payout.manager.PayoutCancelled;
import dev.vality.payout.manager.PayoutStatus;
import org.springframework.stereotype.Service;

@Service
public class PayoutRowFactory {

    public PayoutRow create(
            Event event,
            String payoutId,
            PayoutStatus payoutStatus) {
        Payout payout = event.getPayout();
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId(payoutId);
        payoutRow.setPayoutToolId(payout.getPayoutToolId());
        payoutRow.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        payoutRow.setPayoutId(payout.getPayoutId());
        payoutRow.setPartyId(payout.getPartyId());
        payoutRow.setShopId(payout.getShopId());
        payoutRow.setPayoutTime(TypeUtil.stringToLocalDateTime(payout.getCreatedAt()));
        payoutRow.setStatus(TBaseUtil.unionFieldToEnum(payoutStatus,
                dev.vality.analytics.constant.PayoutStatus.class)
        );
        payoutRow.setAmount(payout.getAmount());
        payoutRow.setFee(payout.getFee());
        payoutRow.setCurrency(payout.getCurrency().getSymbolicCode());

        if (payoutStatus.isSetCancelled()) {
            PayoutCancelled cancelled = payoutStatus.getCancelled();
            payoutRow.setStatusCancelledDetails(cancelled.getDetails());
        }

        return payoutRow;
    }
}
