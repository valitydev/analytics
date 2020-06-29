package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.constant.PayoutStatus;
import com.rbkmoney.analytics.dao.model.AdjustmentRow;
import com.rbkmoney.analytics.dao.model.PaymentRow;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.model.RefundRow;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresBalanceChangesRepository;
import com.rbkmoney.analytics.domain.CashFlowResult;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = AnalyticsApplication.class, initializers = PostgresRepositoryTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class PostgresRepositoryTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    @LocalServerPort
    protected int port;

    @Autowired
    private PostgresBalanceChangesRepository postgresBalanceChangesRepository;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword())
                    .and(configurableApplicationContext.getEnvironment().getActiveProfiles())
                    .applyTo(configurableApplicationContext);
        }
    }

    @Test
    public void testCount() {
        postgresBalanceChangesRepository.insertPayments(List.of(payment()));
        postgresBalanceChangesRepository.insertRefunds(List.of(refund()));
        postgresBalanceChangesRepository.insertAdjustments(List.of(adjustment()));
        postgresBalanceChangesRepository.insertPayouts(List.of(payout()));

        long count = postgresJdbcTemplate.queryForObject(
                "SELECT count(*) AS count FROM analytics.balance_change",
                (resultSet, i) -> resultSet.getLong("count"));
        assertEquals(4L, count);
    }

    private PaymentRow payment() {
        PaymentRow paymentRow = new PaymentRow();
        paymentRow.setInvoiceId("invoice_id");
        paymentRow.setSequenceId(1L);
        paymentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        paymentRow.setCurrency("RUB");
        paymentRow.setPartyId("party_id");
        paymentRow.setShopId("shop_id");
        paymentRow.setCashFlowResult(CashFlowResult.builder()
                .amount(1000L)
                .systemFee(100L)
                .build());

        return paymentRow;
    }

    private RefundRow refund() {
        RefundRow refundRow = new RefundRow();
        refundRow.setInvoiceId("invoice_id");
        refundRow.setSequenceId(2L);
        refundRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        refundRow.setCurrency("RUB");
        refundRow.setPartyId("party_id");
        refundRow.setShopId("shop_id");
        refundRow.setCashFlowResult(CashFlowResult.builder()
                .amount(500L)
                .systemFee(50L)
                .build());

        return refundRow;
    }

    private AdjustmentRow adjustment() {
        AdjustmentRow adjustmentRow = new AdjustmentRow();
        adjustmentRow.setInvoiceId("invoice_id");
        adjustmentRow.setSequenceId(3L);
        adjustmentRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        adjustmentRow.setCurrency("RUB");
        adjustmentRow.setPartyId("party_id");
        adjustmentRow.setShopId("shop_id");
        adjustmentRow.setCashFlowResult(CashFlowResult.builder()
                .systemFee(250L)
                .build());
        adjustmentRow.setOldCashFlowResult(CashFlowResult.builder()
                .systemFee(100L)
                .build());

        return adjustmentRow;
    }

    private PayoutRow payout() {
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId("pauout_id");
        payoutRow.setStatus(PayoutStatus.paid);
        payoutRow.setEventTime(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
        payoutRow.setCurrency("RUB");
        payoutRow.setPartyId("party_id");
        payoutRow.setShopId("shop_id");
        payoutRow.setAmount(10_000L);
        payoutRow.setFee(1000L);

        return payoutRow;
    }
}
