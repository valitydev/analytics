package dev.vality.analytics.listener.handler.invoice;

import dev.vality.analytics.dao.model.RefundRow;
import dev.vality.analytics.dao.repository.RepositoryFacade;
import dev.vality.analytics.listener.Processor;
import dev.vality.analytics.listener.mapper.invoice.RefundMapper;
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
public class RefundBatchHandler implements InvoiceBatchHandler {

    private final RepositoryFacade repositoryFacade;
    private final List<RefundMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<RefundMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<RefundRow> invoiceEvents = changes.stream()
                .map(changeWithParent -> {
                    InvoiceChange change = changeWithParent.getValue();
                    for (RefundMapper invoiceMapper : getMappers()) {
                        if (invoiceMapper.accept(change)) {
                            return invoiceMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertRefunds(invoiceEvents);
    }
}
