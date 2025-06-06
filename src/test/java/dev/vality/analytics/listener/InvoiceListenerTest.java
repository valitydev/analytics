package dev.vality.analytics.listener;

import dev.vality.analytics.config.SpringBootITest;
import dev.vality.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import dev.vality.analytics.utils.BuildUtils;
import dev.vality.analytics.utils.EventRangeFactory;
import dev.vality.columbus.ColumbusServiceSrv;
import dev.vality.damsel.domain.*;
import dev.vality.damsel.payment_processing.InvoicingSrv;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootITest
public class InvoiceListenerTest {

    private static final long MESSAGE_TIMEOUT = 4_000L;
    private static final String SOURCE_ID = "sourceID";
    private static final String FIRST = "1";
    private static final String SELECT_SUM = "SELECT shopId, sum(amount) as sum " +
            "from %1s " +
            "group by shopId, currency, status " +
            "having shopId = '";

    @Value("${kafka.topic.event.sink.initial}")
    public String eventSinkTopic;

    @MockitoBean
    private ColumbusServiceSrv.Iface iface;
    @MockitoBean
    private InvoicingSrv.Iface invoicingClient;
    @MockitoBean
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;
    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;
    @Autowired
    private EventRangeFactory eventRangeFactory;
    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Test
    public void testEventSink() throws InterruptedException, IOException, TException {
        List<SinkEvent> sinkEvents = InvoiceFlowGenerator.generateSuccessFlow(SOURCE_ID);

        mockPayment(SOURCE_ID);

        sinkEvents.forEach(event -> testThriftKafkaProducer.send(eventSinkTopic, event));

        sinkEvents = InvoiceFlowGenerator.generateSuccessNotFullFlow("sourceID_2");
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(eventSinkTopic, event));

        AtomicLong count = new AtomicLong();

        //check sum for captured payment
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);

            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink") + InvoiceFlowGenerator.SHOP_ID +
                                "' and status = 'captured' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 1000L;
        });

        //statistic for paymentTool
        List<Map<String, Object>> list = clickHouseJdbcTemplate.queryForList(
                "SELECT shopId, paymentTool," +
                        "( SELECT count() from analytic.events_sink " +
                        "group by shopId, currency " +
                        "having shopId = '" + InvoiceFlowGenerator.SHOP_ID +
                        "' and currency = 'RUB') as total_count, " +
                        "count() * 100 / total_count as sum " +
                        "from analytic.events_sink " +
                        "group by shopId, currency, paymentTool " +
                        "having shopId = '" + InvoiceFlowGenerator.SHOP_ID + "' and currency = 'RUB'");

        list.forEach(stringObjectMap -> {
                    Object cnt = stringObjectMap.get("sum");
                    assertEquals(100.0, cnt);
                    System.out.println(stringObjectMap);
                }
        );

        String sourceIDRefundFirst = "sourceID_refund_1";
        mockPayment(sourceIDRefundFirst);
        mockRefund(sourceIDRefundFirst, 8, "1");
        mockRefund(sourceIDRefundFirst, 10, "2");

        // test refund flow
        sinkEvents = InvoiceFlowGenerator.generateRefundedFlow(sourceIDRefundFirst);
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(eventSinkTopic, event));

        //check sum for succeeded refund
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_refund") + InvoiceFlowGenerator.SHOP_ID +
                                "' and status = 'succeeded' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 246L;
        });

        //check collapsing sum for pending refund
        List<Map<String, Object>> resultList = clickHouseJdbcTemplate.queryForList(
                String.format(SELECT_SUM, "analytic.events_sink_refund") + InvoiceFlowGenerator.SHOP_ID +
                        "' and status = 'pending' and currency = 'RUB'");

        assertTrue(resultList.isEmpty());

        String sourceAdjustment = "source_adjustment";
        mockPayment(sourceAdjustment);
        mockAdjustment(sourceAdjustment, 7, FIRST);

        sinkEvents = InvoiceFlowGenerator.generateSuccessAdjustment(sourceAdjustment);
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(eventSinkTopic, event));

        //check sum for succeeded refund
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(MESSAGE_TIMEOUT);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_adjustment") +
                                InvoiceFlowGenerator.SHOP_ID + "' and status = 'captured' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 23L;
        });

        String sourceChargeback = "source_chargeback";
        mockPayment(sourceChargeback);
        mockChargeback(sourceChargeback, 7, FIRST);
        sinkEvents = InvoiceFlowGenerator.generateChargebackFlow(sourceChargeback);
        sinkEvents.forEach(event -> testThriftKafkaProducer.send(eventSinkTopic, event));


        //check sum for succeeded chargeback
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Thread.sleep(20000L);
            try {
                count.set(clickHouseJdbcTemplate.queryForObject(
                        String.format(SELECT_SUM, "analytic.events_sink_chargeback") +
                                InvoiceFlowGenerator.SHOP_ID + "' and status = 'accepted' and currency = 'RUB'",
                        (resultSet, i) -> resultSet.getLong("sum")));
            } catch (Exception e) {
                return false;
            }

            return count.get() == 23L;
        });

    }

    private void mockPayment(String sourceId) throws TException, IOException {
        Mockito.when(invoicingClient.get(sourceId, eventRangeFactory.create(6)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", FIRST, FIRST,
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.pending(new InvoicePaymentPending())));
    }

    private void mockRefund(String sourceId, int sequenceId, String refundId) throws TException, IOException {
        Mockito.when(invoicingClient.get(sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", refundId, "1", "1",
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.refunded(new InvoicePaymentRefunded())));
    }

    private void mockAdjustment(String sourceId, int sequenceId, String adjustmentId) throws TException, IOException {
        Mockito.when(invoicingClient.get(sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", "1", adjustmentId,
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.captured(new InvoicePaymentCaptured())));
    }

    private void mockChargeback(String sourceId, int sequenceId, String chargebackId) throws TException, IOException {
        Mockito.when(invoicingClient.get(sourceId, eventRangeFactory.create(sequenceId)))
                .thenReturn(BuildUtils.buildInvoice(InvoiceFlowGenerator.PARTY_ID, InvoiceFlowGenerator.SHOP_ID,
                        sourceId, "1", "1", chargebackId, "1",
                        InvoiceStatus.paid(new InvoicePaid()),
                        InvoicePaymentStatus.charged_back(new InvoicePaymentChargedBack())));
    }
}
