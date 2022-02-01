package dev.vality.analytics.listener.handler.payout;

import dev.vality.analytics.dao.model.PayoutRow;
import dev.vality.analytics.dao.repository.RepositoryFacade;
import dev.vality.analytics.listener.Processor;
import dev.vality.analytics.listener.handler.BatchHandler;
import dev.vality.analytics.listener.mapper.payout.PayoutMapper;
import dev.vality.payout.manager.Event;
import dev.vality.payout.manager.PayoutChange;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class PayoutBatchHandler implements BatchHandler<PayoutChange, Event> {

    private final RepositoryFacade repositoryFacade;
    private final List<PayoutMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<PayoutMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<Event, PayoutChange>> changes) {
        List<PayoutRow> payoutRows = changes.stream()
                .map(changeWithParent -> {
                    PayoutChange change = changeWithParent.getValue();
                    for (PayoutMapper payoutMapper : getMappers()) {
                        if (payoutMapper.accept(change)) {
                            return payoutMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertPayouts(payoutRows);
    }
}