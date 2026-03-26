package dev.vality.analytics.listener;

import dev.vality.analytics.converter.SourceEventParser;
import dev.vality.analytics.listener.handler.invoice.InvoiceBatchHandler;
import dev.vality.damsel.payment_processing.InvoiceChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceListener {

    private final BatchRetryErrorListener batchRetryErrorListener;
    private final SourceEventParser eventParser;
    private final List<InvoiceBatchHandler> invoiceBatchHandlers;

    @KafkaListener(
            autoStartup = "${kafka.listener.event.sink.enabled}",
            topics = "${kafka.topic.event.sink.initial}",
            containerFactory = "invoiceListenerContainerFactory")
    public void listen(
            List<MachineEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) int offsets,
            Acknowledgment ack) {
        log.info("InvoiceListener listen offsets: {}, partition: {}, batch.size: {}", offsets, partition, batch.size());
        if (CollectionUtils.isEmpty(batch)) {
            ack.acknowledge();
            return;
        }

        try {
            handleMessages(batch);
            ack.acknowledge();
        } catch (Exception ex) {
            batchRetryErrorListener.retry("invoice-events", batch.size(), ack, ex);
        }
    }

    private void handleMessages(List<MachineEvent> batch) {
        batch.stream()
                .map(machineEvent -> Map.entry(machineEvent, eventParser.parseEvent(machineEvent)))
                .filter(entry -> entry.getValue().isSetInvoiceChanges())
                .map(entry -> entry.getValue().getInvoiceChanges().stream()
                        .map(invoiceChange -> Map.entry(entry.getKey(), invoiceChange))
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(groupingBy(
                        entry -> Optional.ofNullable(getHandler(entry.getValue())),
                        toList()))
                .forEach((handler, entries) -> handler
                        .ifPresent(eventBatchHandler -> eventBatchHandler.handle(entries).execute()));
    }

    private InvoiceBatchHandler getHandler(InvoiceChange change) {
        for (InvoiceBatchHandler handler : invoiceBatchHandlers) {
            if (handler.accept(change)) {
                return handler;
            }
        }

        return null;
    }
}
