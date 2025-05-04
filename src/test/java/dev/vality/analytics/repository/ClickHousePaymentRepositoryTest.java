package dev.vality.analytics.repository;

import dev.vality.analytics.config.ClickHouseConfig;
import dev.vality.analytics.config.ClickhouseTest;
import dev.vality.analytics.config.RawMapperConfig;
import dev.vality.analytics.converter.*;
import dev.vality.analytics.dao.mapper.SplitRowsMapper;
import dev.vality.analytics.dao.mapper.SplitStatusRowsMapper;
import dev.vality.analytics.dao.model.NamingDistribution;
import dev.vality.analytics.dao.model.NumberModel;
import dev.vality.analytics.dao.model.SplitNumberModel;
import dev.vality.analytics.dao.model.SplitStatusNumberModel;
import dev.vality.analytics.dao.repository.clickhouse.ClickHousePaymentRepositoryImpl;
import dev.vality.analytics.utils.constant.PaymentsWithFeeConstants;
import dev.vality.damsel.analytics.SplitUnit;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ContextConfiguration(
        classes = {
                RawToNumModelConverter.class, RawToSplitNumberConverter.class,
                RawToSplitStatusConverter.class, RawToShopAmountModelConverter.class,
                SplitRowsMapper.class, SplitStatusRowsMapper.class, RawToNamingDistributionConverter.class,
                RawMapperConfig.class, ClickHousePaymentRepositoryImpl.class, JdbcTemplateAutoConfiguration.class,
                ClickHouseConfig.class})
@EnableConfigurationProperties
@ClickhouseTest
public class ClickHousePaymentRepositoryTest {

    public static final String RUB = "RUB";
    @Autowired
    private ClickHousePaymentRepositoryImpl clickHousePaymentRepository;

    @Test
    public void testAmountListPayment() {
        List<NumberModel> numberModels =
                clickHousePaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                        null, null,
                        Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        NumberModel numberModel = findCost(numberModels, RUB);

        assertEquals(6000L, numberModel.getNumber().longValue());
        numberModels = clickHousePaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());


        numberModel = findCost(numberModels, RUB);

        assertEquals(5000L, numberModel.getNumber().longValue());
    }

    @Test
    public void testSplitAmountListPayment() {
        List<SplitNumberModel> costs =
                clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                        null, null,
                        Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        SplitUnit.MINUTE);

        assertEquals(3, costs.size());
        NumberModel numberModel = findCost(costs, RUB);
        assertEquals(1000L, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.HOUR);
        assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(1000L, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.DAY);
        assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(1000L, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.WEEK);
        assertEquals(1, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(3000L, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.MONTH);
        numberModel = findCost(costs, RUB);
        assertEquals(3000L, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.YEAR);
        numberModel = findCost(costs, RUB);
        assertEquals(3000L, numberModel.getNumber().longValue());
    }


    @Test
    public void testSplitCountListPayment() {
        List<SplitStatusNumberModel> costs =
                clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                        null, null,
                        Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        SplitUnit.MINUTE);
        assertEquals(3, costs.size());
        NumberModel numberModel = findCost(costs, RUB);
        assertEquals(1, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.HOUR);
        assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(1, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.DAY);
        assertEquals(3, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(1, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.WEEK);
        assertEquals(1, costs.size());
        numberModel = findCost(costs, RUB);
        assertEquals(3, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.MONTH);
        numberModel = findCost(costs, RUB);
        assertEquals(3, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.YEAR);
        numberModel = findCost(costs, RUB);
        assertEquals(3, numberModel.getNumber().longValue());

        costs = clickHousePaymentRepository.getPaymentsSplitCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772d",
                null, List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"),
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000698L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                SplitUnit.YEAR);

        Assertions.assertTrue(costs.isEmpty());
    }

    @NotNull
    private NumberModel findCost(List<? extends NumberModel> costs, String currency) {
        return costs.stream()
                .filter(costEntity -> currency.equals(costEntity.getCurrency()))
                .findFirst()
                .get();
    }

    @Test
    public void testAveragePayment() {
        List<NumberModel> numberModels =
                clickHousePaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                        null, null,
                        Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());
        NumberModel numberModel = findCost(numberModels, RUB);
        assertEquals(3000L, numberModel.getNumber().longValue());

        numberModels = clickHousePaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());
        numberModel = findCost(numberModels, RUB);
        assertEquals(5000L, numberModel.getNumber().longValue());

        numberModels = clickHousePaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                null, List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"),
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());
        numberModel = findCost(numberModels, RUB);
        assertEquals(1000L, numberModel.getNumber().longValue());

        numberModels = clickHousePaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b173342",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());
        assertTrue(numberModels.isEmpty());
    }

    @Test
    public void testCountPayment() {
        List<NumberModel> countModels =
                clickHousePaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                        null, null,
                        Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(1579666000697L).atZone(ZoneOffset.UTC).toLocalDateTime());
        NumberModel countModel = findCountModel(countModels, RUB);
        assertEquals(2, countModel.getNumber().longValue());

        countModels = clickHousePaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        countModel = findCountModel(countModels, RUB);
        assertEquals(1, countModel.getNumber().longValue());

        countModels = clickHousePaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b173342",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1579666000697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        assertTrue(countModels.isEmpty());
    }

    @NotNull
    private NumberModel findCountModel(List<NumberModel> countModels, String currency) {
        return countModels.stream()
                .filter(count -> currency.equals(count.getCurrency()))
                .findFirst()
                .get();
    }

    @Test
    public void testPaymentToolDistr() {
        List<NamingDistribution> toolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        toolDistribution.forEach(paymentToolDistribution -> {
                    if (paymentToolDistribution.getName().equals("bank_card")) {
                        assertEquals(33, paymentToolDistribution.getPercent().intValue());
                    }
                    if (paymentToolDistribution.getName().equals("payment_terminal")) {
                        assertEquals(66, paymentToolDistribution.getPercent().intValue());
                    }
                }
        );

        toolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        toolDistribution.forEach(paymentToolDistribution ->
                assertEquals(100, paymentToolDistribution.getPercent().intValue())
        );

        toolDistribution = clickHousePaymentRepository.getPaymentsToolDistribution(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                null, List.of("ad8b7bfd-0760-4781-a400-51903ee8e503", "ad8b7bfd-0760-4781-a400-51903ee8e502"),
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        Assertions.assertTrue(toolDistribution.isEmpty());
    }

    @Test
    public void testPaymentErrorDistr() {
        List<NamingDistribution> errorDistribution = clickHousePaymentRepository.getPaymentsErrorReasonDistribution(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772k",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        Optional<NamingDistribution> errorResult = errorDistribution.stream()
                .filter(error -> "card is failed".equals(error.getName()))
                .findFirst();
        assertEquals(errorResult.get().getPercent(), Double.valueOf(33.33));
    }

    @Test
    public void testPaymentErrorCodeDistr() {
        List<NamingDistribution> errorDistribution = clickHousePaymentRepository.getPaymentsErrorCodeDistribution(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772k",
                null, null,
                Instant.ofEpochMilli(1575554400000L).atZone(ZoneOffset.UTC).toLocalDateTime(),
                Instant.ofEpochMilli(1575556887697L).atZone(ZoneOffset.UTC).toLocalDateTime());

        Optional<NamingDistribution> errorResult = errorDistribution.stream()
                .filter(error -> "authorization_failed:rejected_by_issuer".equals(error.getName()))
                .findFirst();
        assertEquals(errorResult.get().getPercent(), Double.valueOf(33.33));
    }

    @Test
    public void testGetCurrentBalances() {
        List<NumberModel> currentBalances = clickHousePaymentRepository.getCurrentBalances(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772d", null, List.of());

        NumberModel countModel = findCountModel(currentBalances, RUB);
        assertEquals(3000L, countModel.getNumber().longValue());

        currentBalances = clickHousePaymentRepository.getCurrentBalances(
                "ca2e9162-eda2-4d17-bbfa-dc5e39b1772f", null, List.of());

        countModel = findCountModel(currentBalances, RUB);
        assertEquals(49900L, countModel.getNumber().longValue());

        currentBalances = clickHousePaymentRepository.getCurrentBalances("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e509"), List.of());

        countModel = findCountModel(currentBalances, RUB);
        assertEquals(44900L, countModel.getNumber().longValue());

        currentBalances = clickHousePaymentRepository.getCurrentBalances("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e509", "ad8b7bfd-0760-4781-a400-51903ee8e501"), List.of());

        countModel = findCountModel(currentBalances, RUB);
        assertEquals(49900L, countModel.getNumber().longValue());

        currentBalances = clickHousePaymentRepository.getCurrentBalances("ca2e9162-eda2-4d17-bbfa-dc5e39b1772f",
                List.of(), List.of("ad8b7bfd-0760-4781-a400-51903ee8e501"));

        countModel = findCountModel(currentBalances, RUB);
        assertEquals(44900L, countModel.getNumber().longValue());

        currentBalances = clickHousePaymentRepository.getCurrentBalances("test", null, List.of());

        assertTrue(currentBalances.isEmpty());
    }

    @Test
    public void testGetCreditings() {
        List<NumberModel> creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                null,
                null,
                PaymentsWithFeeConstants.BEFORE_FIRST_TIMESTAMP,
                PaymentsWithFeeConstants.AFTER_THIRD_TIMESTAMP
        );
        assertCreditings(2400L, creditings);

        creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                null,
                List.of(PaymentsWithFeeConstants.FIRST_SHOP_ID, PaymentsWithFeeConstants.SECOND_SHOP_ID),
                PaymentsWithFeeConstants.BEFORE_FIRST_TIMESTAMP,
                PaymentsWithFeeConstants.AFTER_THIRD_TIMESTAMP
        );
        assertCreditings(900L, creditings);

        creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                List.of(PaymentsWithFeeConstants.SECOND_SHOP_ID, PaymentsWithFeeConstants.THIRD_SHOP_ID),
                null,
                PaymentsWithFeeConstants.BEFORE_FIRST_TIMESTAMP,
                PaymentsWithFeeConstants.AFTER_THIRD_TIMESTAMP
        );
        assertCreditings(1700L, creditings);

        creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                List.of(PaymentsWithFeeConstants.FIRST_SHOP_ID),
                List.of(PaymentsWithFeeConstants.FIRST_SHOP_ID, PaymentsWithFeeConstants.THIRD_SHOP_ID),
                PaymentsWithFeeConstants.BEFORE_FIRST_TIMESTAMP,
                PaymentsWithFeeConstants.AFTER_THIRD_TIMESTAMP
        );
        assertTrue(creditings.isEmpty());

        creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                null,
                List.of(PaymentsWithFeeConstants.FIRST_SHOP_ID),
                PaymentsWithFeeConstants.BEFORE_FIRST_TIMESTAMP,
                PaymentsWithFeeConstants.BEFORE_THIRD_TIMESTAMP
        );
        assertCreditings(800L, creditings);

        creditings = clickHousePaymentRepository.getCreditings(
                PaymentsWithFeeConstants.PARTY_ID,
                List.of(PaymentsWithFeeConstants.SECOND_SHOP_ID, PaymentsWithFeeConstants.THIRD_SHOP_ID),
                null,
                PaymentsWithFeeConstants.BEFORE_THIRD_TIMESTAMP,
                PaymentsWithFeeConstants.AFTER_THIRD_TIMESTAMP
        );
        assertCreditings(900L, creditings);
    }

    private void assertCreditings(Long value, List<NumberModel> creditings) {
        assertEquals(1, creditings.size());
        assertEquals(value, creditings.get(0).getNumber());
        assertEquals("RUB", creditings.get(0).getCurrency());
    }
}
