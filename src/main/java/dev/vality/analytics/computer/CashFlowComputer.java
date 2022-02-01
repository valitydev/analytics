package dev.vality.analytics.computer;

import dev.vality.analytics.domain.CashFlowResult;
import dev.vality.damsel.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
public class CashFlowComputer {

    public CashFlowResult compute(List<FinalCashFlowPosting> cashFlow) {

        long amount = 0L;
        long systemFee = 0L;
        long providerFee = 0L;
        long externalFee = 0L;
        long guaranteeDeposit = 0L;

        if (CollectionUtils.isEmpty(cashFlow)) {
            return CashFlowResult.EMPTY;
        }

        for (FinalCashFlowPosting posting : cashFlow) {
            if (!posting.isSetSource() || !posting.isSetDestination()) {
                continue;
            }

            if (isPayment(posting)) {
                amount += posting.getVolume().getAmount();
            }

            if (isRefund(posting)) {
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

            if (isGuaranteeDeposit(posting)) {
                guaranteeDeposit += posting.getVolume().getAmount();
            }

        }

        return CashFlowResult.builder()
                .amount(amount)
                .systemFee(systemFee)
                .providerFee(providerFee)
                .externalFee(externalFee)
                .guaranteeDeposit(guaranteeDeposit)
                .build();
    }

    private boolean isPayment(FinalCashFlowPosting posting) {
        log.debug("CashFlowComputer isPayment posting: {}", posting);
        return posting.getSource().getAccountType().isSetProvider() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isSettlement(posting.getDestination());
    }

    private boolean isRefund(FinalCashFlowPosting posting) {
        log.debug("CashFlowComputer isRefund posting: {}", posting);
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private boolean isSystemFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetSystem() && isSettlement(posting.getDestination());
    }

    private boolean isProviderFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetProvider() && isSettlement(posting.getDestination());
    }

    private boolean isExternalFee(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetSystem() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetExternal() && isExternal(posting.getDestination());
    }

    private boolean isGuaranteeDeposit(FinalCashFlowPosting posting) {
        return posting.getSource().getAccountType().isSetMerchant() && isSettlement(posting.getSource())
                && posting.getDestination().getAccountType().isSetMerchant() && isGuarantee(posting.getDestination());
    }

    private boolean isSettlement(FinalCashFlowAccount account) {
        return (account.getAccountType().isSetMerchant()
                && account.getAccountType().getMerchant() == MerchantCashFlowAccount.settlement)
                || (account.getAccountType().isSetProvider()
                && account.getAccountType().getProvider() == ProviderCashFlowAccount.settlement)
                || (account.getAccountType().isSetSystem()
                && account.getAccountType().getSystem() == SystemCashFlowAccount.settlement);
    }

    private boolean isExternal(FinalCashFlowAccount account) {
        return account.getAccountType().getExternal() == ExternalCashFlowAccount.income
                || account.getAccountType().getExternal() == ExternalCashFlowAccount.outcome;
    }

    private boolean isGuarantee(FinalCashFlowAccount account) {
        return account.getAccountType().getMerchant() == MerchantCashFlowAccount.guarantee;
    }
}
