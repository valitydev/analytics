package dev.vality.analytics.listener.mapper.payout;

import dev.vality.analytics.constant.EventType;
import dev.vality.analytics.dao.model.PayoutRow;
import dev.vality.analytics.dao.repository.clickhouse.ClickHousePayoutRepository;
import dev.vality.analytics.listener.mapper.Mapper;
import dev.vality.analytics.listener.mapper.factory.PayoutRowFactory;
import dev.vality.payout.manager.Event;
import dev.vality.payout.manager.PayoutChange;
import dev.vality.payout.manager.PayoutStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutMapper implements Mapper<PayoutChange, Event, PayoutRow> {

    private final PayoutRowFactory payoutRowFactory;
    private final ClickHousePayoutRepository clickHousePayoutRepository;

    @Override
    public PayoutRow map(PayoutChange change, Event event) {
        String payoutId = event.getPayoutId();
        PayoutStatus payoutStatus = change.getStatusChanged().getStatus();
        PayoutRow payoutRow = payoutRowFactory.create(event, payoutId, payoutStatus);
        if (payoutStatus.isSetCancelled()) {
            String paidEvent = clickHousePayoutRepository.getPaidEvent(payoutId);
            payoutRow.setCancelledAfterBeingPaid(paidEvent != null);
        }
        return payoutRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PAYOUT_STATUS_CHANGED;
    }
}
