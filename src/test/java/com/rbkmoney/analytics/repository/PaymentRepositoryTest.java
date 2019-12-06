package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.converter.RawToCostConverter;
import com.rbkmoney.analytics.converter.RawToCountModelConverter;
import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.converter.RawToSplitCostConverter;
import com.rbkmoney.analytics.dao.model.Cost;
import com.rbkmoney.analytics.dao.model.CountModel;
import com.rbkmoney.analytics.dao.model.NamingDistribution;
import com.rbkmoney.analytics.dao.model.SplitCost;
import com.rbkmoney.analytics.dao.repository.MgPaymentRepository;
import com.rbkmoney.damsel.analytics.SplitUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = PaymentRepositoryTest.Initializer.class,
        classes = {RawToCostConverter.class, RawToCountModelConverter.class, RawToSplitCostConverter.class,
                RawToNamingDistributionConverter.class, RawMapperConfig.class, MgPaymentRepository.class})
public class PaymentRepositoryTest extends ClickhouseAbstractTest {

    @Autowired
    private MgPaymentRepository mgPaymentRepository;

    @Test
    public void testAmountListPayment() {
        List<Cost> costs = mgPaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);

        Cost cost = costs.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(6000L, cost.getAmount().longValue());
        costs = mgPaymentRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);

        cost = costs.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(5000L, cost.getAmount().longValue());
    }

    @Test
    public void testSplitAmountListPayment() {
        List<SplitCost> costs = mgPaymentRepository.getPaymentsSplitAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772k", null, 1575554400000L, 1575599997697L, SplitUnit.MINUTE);

        Cost cost = costs.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();

        Assert.assertEquals(3000L, cost.getAmount().longValue());
    }

    @Test
    public void testAveragePayment() {
        List<Cost> costs = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);
        Cost cost = costs.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(3000L, cost.getAmount().longValue());

        costs = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);
        cost = costs.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(5000L, cost.getAmount().longValue());

        costs = mgPaymentRepository.getAveragePayment("ca2e9162-eda2-4d17-bbfa-dc5e39b173342", null, 1575554400000L, 1575556887697L);
        Assert.assertTrue(costs.isEmpty());
    }

    @Test
    public void testCountPayment() {
        List<CountModel> countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);
        CountModel countModel = countModels.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(2, countModel.getCount().longValue());

        countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);
        countModel = countModels.stream()
                .filter(costEntity -> "RUB".equals(costEntity.getCurrency()))
                .findFirst()
                .get();
        Assert.assertEquals(1, countModel.getCount().longValue());

        countModels = mgPaymentRepository.getPaymentsCount("ca2e9162-eda2-4d17-bbfa-dc5e39b173342", null, 1575554400000L, 1575556887697L);
        Assert.assertTrue(countModels.isEmpty());
    }

    @Test
    public void testPaymentToolDistr() {
        List<NamingDistribution> toolDistribution = mgPaymentRepository.getPaymentsToolDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", null, 1575554400000L, 1575556887697L);

        toolDistribution.forEach(paymentToolDistribution ->
                Assert.assertEquals(50, paymentToolDistribution.getPercent().intValue())
        );

        toolDistribution = mgPaymentRepository.getPaymentsToolDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a",
                List.of("ad8b7bfd-0760-4781-a400-51903ee8e502", "ad8b7bfd-0760-4781-a400-51903ee8e503"), 1575554400000L, 1575556887697L);

        toolDistribution.forEach(paymentToolDistribution ->
                Assert.assertEquals(50, paymentToolDistribution.getPercent().intValue())
        );
    }

    @Test
    public void testPaymentErrorDistr() {
        List<NamingDistribution> errorDistribution = mgPaymentRepository.getPaymentsErrorDistribution("ca2e9162-eda2-4d17-bbfa-dc5e39b1772k", null, 1575554400000L, 1575599987697L);

        Optional<NamingDistribution> errorResult = errorDistribution.stream()
                .filter(error -> "card is failed".equals(error.getName()))
                .findFirst();
        Assert.assertEquals(errorResult.get().getPercent(), Double.valueOf(33.33));
    }

}
