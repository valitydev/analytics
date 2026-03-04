package dev.vality.analytics.computer;

import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.fistful.cashflow.ExternalCashFlowAccount;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.cashflow.MerchantCashFlowAccount;
import dev.vality.fistful.cashflow.ProviderCashFlowAccount;
import dev.vality.fistful.cashflow.SystemCashFlowAccount;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Service
public class WithdrawalCashFlowComputer {

    public CashFlowResult compute(List<FinalCashFlowPosting> cashFlow) {
        long amount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;

        if (CollectionUtils.isEmpty(cashFlow)) {
            return CashFlowResult.EMPTY;
        }

        for (FinalCashFlowPosting posting : cashFlow) {
            if (posting == null || !posting.isSetSource() || !posting.isSetDestination() || !posting.isSetVolume()) {
                continue;
            }

            if (isAmount(posting)) {
                amount += posting.getVolume().getAmount();
            }

            if (isSystemFee(posting)) {
                systemFee += posting.getVolume().getAmount();
            }

            if (isProviderFee(posting)) {
                providerFee += posting.getVolume().getAmount();
            }

            if (isExternalFee(posting)) {
                externalFee += posting.getVolume().getAmount();
            }
        }

        return CashFlowResult.builder()
                .amount(amount)
                .guaranteeDeposit(0L)
                .systemFee(systemFee)
                .providerFee(providerFee)
                .externalFee(externalFee)
                .build();
    }

    private boolean isAmount(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant()
                && posting.getSource().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
                && posting.getDestination().getAccountType().isSetMerchant()
                && posting.getDestination().getAccountType().getMerchant() == MerchantCashFlowAccount.payout;
    }

    private boolean isSystemFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant()
                && posting.getSource().getAccountType().getMerchant() == MerchantCashFlowAccount.settlement
                && posting.getDestination().getAccountType().isSetSystem()
                && posting.getDestination().getAccountType().getSystem() == SystemCashFlowAccount.settlement;
    }

    private boolean isProviderFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem()
                && posting.getSource().getAccountType().getSystem() == SystemCashFlowAccount.settlement
                && posting.getDestination().getAccountType().isSetProvider()
                && posting.getDestination().getAccountType().getProvider() == ProviderCashFlowAccount.settlement;
    }

    private boolean isExternalFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem()
                && posting.getSource().getAccountType().getSystem() == SystemCashFlowAccount.settlement
                && posting.getDestination().getAccountType().isSetExternal()
                && (posting.getDestination().getAccountType().getExternal() == ExternalCashFlowAccount.income
                || posting.getDestination().getAccountType().getExternal() == ExternalCashFlowAccount.outcome);
    }
}
