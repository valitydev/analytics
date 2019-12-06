package com.rbkmoney.analytics.repository;

import com.rbkmoney.analytics.config.RawMapperConfig;
import com.rbkmoney.analytics.converter.RawToCostConverter;
import com.rbkmoney.analytics.converter.RawToCountModelConverter;
import com.rbkmoney.analytics.converter.RawToNamingDistributionConverter;
import com.rbkmoney.analytics.dao.model.Cost;
import com.rbkmoney.analytics.dao.repository.MgRefundRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = RefundRepositoryTest.Initializer.class,
        classes = {RawToCostConverter.class, RawToCountModelConverter.class, RawToNamingDistributionConverter.class,
                RawMapperConfig.class, MgRefundRepository.class})
public class RefundRepositoryTest extends ClickhouseAbstractTest {

    @Autowired
    private MgRefundRepository mgRefundRepository;

    @Test
    public void testAmountListRefundByRepository() {
        List<Cost> costs = mgRefundRepository.getPaymentsAmount("ca2e9162-eda2-4d17-bbfa-dc5e39b1772a", List.of("ad8b7bfd-0760-4781-a400-51903ee8e502"), 1575554400000L, 1575556887697L);
        Assert.assertTrue(costs.isEmpty());
    }
}
