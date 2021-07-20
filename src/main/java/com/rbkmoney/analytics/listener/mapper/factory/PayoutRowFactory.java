package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.payout.manager.Event;
import com.rbkmoney.payout.manager.Payout;
import com.rbkmoney.payout.manager.PayoutCancelled;
import com.rbkmoney.payout.manager.PayoutStatus;
import org.springframework.stereotype.Service;

@Service
public class PayoutRowFactory {

    public PayoutRow create(
            Event event,
            Payout payoutCreated,
            String payoutId,
            PayoutStatus payoutStatus) {
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId(payoutId);
        payoutRow.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        payoutRow.setPayoutId(payoutCreated.getPayoutId());
        payoutRow.setPartyId(payoutCreated.getPartyId());
        payoutRow.setShopId(payoutCreated.getShopId());
        payoutRow.setPayoutTime(TypeUtil.stringToLocalDateTime(payoutCreated.getCreatedAt()));
        payoutRow.setStatus(TBaseUtil.unionFieldToEnum(payoutStatus,
                com.rbkmoney.analytics.constant.PayoutStatus.class)
        );
        payoutRow.setAmount(payoutCreated.getAmount());
        payoutRow.setFee(payoutCreated.getFee());
        payoutRow.setCurrency(payoutCreated.getCurrency().getSymbolicCode());

        if (payoutStatus.isSetCancelled()) {
            PayoutCancelled cancelled = payoutStatus.getCancelled();
            payoutRow.setStatusCancelledDetails(cancelled.getDetails());
        }

        return payoutRow;
    }
}
