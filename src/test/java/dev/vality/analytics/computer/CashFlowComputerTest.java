package dev.vality.analytics.computer;

import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.utils.BuildUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CashFlowComputerTest {

    private CashFlowComputer cashFlowComputer;

    @BeforeEach
    public void setUp() {
        cashFlowComputer = new CashFlowComputer();
    }

    @Test
    public void shouldComputeCashFlowResult() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(BuildUtils.createCashFlow(1000L, 100L));

        // Then
        assertThat(result.getAmount(), is(1000L));
        assertThat(result.getSystemFee(), is(100L));
        assertThat(result.getProviderFee(), is(20L));
        assertThat(result.getExternalFee(), is(10L));
        assertThat(result.getGuaranteeDeposit(), is(100L));
    }

    @Test
    public void shouldHandleNullCashFlow() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(null);

        // Then
        assertThat(result.getAmount(), is(0L));
        assertThat(result.getSystemFee(), is(0L));
        assertThat(result.getProviderFee(), is(0L));
        assertThat(result.getExternalFee(), is(0L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }

    @Test
    public void shouldHandleEmptyCashFlow() {
        // Given - When
        CashFlowResult result = cashFlowComputer.compute(emptyList());

        // Then
        assertThat(result.getAmount(), is(0L));
        assertThat(result.getSystemFee(), is(0L));
        assertThat(result.getProviderFee(), is(0L));
        assertThat(result.getExternalFee(), is(0L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }
}