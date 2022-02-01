package dev.vality.analytics.listener.handler.invoice;

import dev.vality.analytics.dao.model.AdjustmentRow;
import dev.vality.analytics.dao.repository.RepositoryFacade;
import dev.vality.analytics.listener.Processor;
import dev.vality.analytics.listener.mapper.invoice.AdjustmentMapper;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class AdjustmentBatchHandler implements InvoiceBatchHandler {

    private final RepositoryFacade repositoryFacade;
    private final List<AdjustmentMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<AdjustmentMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<AdjustmentRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (AdjustmentMapper mapper : getMappers()) {
                        if (mapper.accept(change)) {
                            return mapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertAdjustments(invoiceEvents);
    }
}
