package dev.vality.analytics.computer;

import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.analytics.utils.WithdrawalEventTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WithdrawalCashFlowComputerTest {

    private WithdrawalCashFlowComputer withdrawalCashFlowComputer;

    @BeforeEach
    public void setUp() {
        withdrawalCashFlowComputer = new WithdrawalCashFlowComputer();
    }

    @Test
    public void shouldMapAllSupportedPostingTypes() {
        CashFlowResult result = withdrawalCashFlowComputer.compute(List.of(
                WithdrawalEventTestUtils.merchantToPayout(1000L),
                WithdrawalEventTestUtils.merchantToSystem(100L),
                WithdrawalEventTestUtils.systemToProvider(20L),
                WithdrawalEventTestUtils.systemToExternal(10L)));

        assertThat(result.getAmount(), is(1000L));
        assertThat(result.getSystemFee(), is(100L));
        assertThat(result.getProviderFee(), is(20L));
        assertThat(result.getExternalFee(), is(10L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }

    @Test
    public void shouldIgnoreUnsupportedPostings() {
        CashFlowResult result = withdrawalCashFlowComputer.compute(List.of(
                WithdrawalEventTestUtils.unrelatedPosting(999L)));

        assertThat(result.getAmount(), is(0L));
        assertThat(result.getSystemFee(), is(0L));
        assertThat(result.getProviderFee(), is(0L));
        assertThat(result.getExternalFee(), is(0L));
        assertThat(result.getGuaranteeDeposit(), is(0L));
    }
}
