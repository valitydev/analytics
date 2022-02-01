package dev.vality.analytics.listener.handler.invoice;

import dev.vality.analytics.dao.model.ChargebackRow;
import dev.vality.analytics.dao.repository.RepositoryFacade;
import dev.vality.analytics.listener.Processor;
import dev.vality.analytics.listener.mapper.invoice.ChargebackMapper;
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
public class ChargebackBatchHandler implements InvoiceBatchHandler {

    private final RepositoryFacade repositoryFacade;
    private final List<ChargebackMapper> mappers;

    @Override
    @SuppressWarnings("unchecked")
    public List<ChargebackMapper> getMappers() {
        return mappers;
    }

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, InvoiceChange>> changes) {
        List<ChargebackRow> invoiceEvents = changes.stream()
                .map(this::findAndMapChange)
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertChargebacks(invoiceEvents);
    }

    private ChargebackRow findAndMapChange(Map.Entry<MachineEvent, InvoiceChange> changeWithParent) {
        InvoiceChange change = changeWithParent.getValue();
        for (ChargebackMapper mapper : getMappers()) {
            if (mapper.accept(change)) {
                return mapper.map(change, changeWithParent.getKey());
            }
        }
        return null;
    }
}
