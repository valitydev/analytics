package dev.vality.analytics.computer;

import dev.vality.analytics.domain.WithdrawalCashFlowResult;
import dev.vality.fistful.cashflow.CashFlowAccount;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.cashflow.MerchantCashFlowAccount;
import dev.vality.fistful.cashflow.ProviderCashFlowAccount;
import dev.vality.fistful.cashflow.SystemCashFlowAccount;
import dev.vality.fistful.cashflow.WalletCashFlowAccount;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.EnumMap;
import java.util.List;

@Service
public class WithdrawalCashFlowComputer {

    public WithdrawalCashFlowResult compute(List<FinalCashFlowPosting> cashFlow) {
        EnumMap<WithdrawalCashFlowType, Long> cashFlowMap = new EnumMap<>(WithdrawalCashFlowType.class);

        if (CollectionUtils.isEmpty(cashFlow)) {
            return WithdrawalCashFlowResult.EMPTY;
        }

        for (FinalCashFlowPosting posting : cashFlow) {
            if (posting == null || !posting.isSetSource() || !posting.isSetDestination() || !posting.isSetVolume()
                    || !posting.getSource().isSetAccountType() || !posting.getDestination().isSetAccountType()) {
                continue;
            }

            WithdrawalCashFlowType type = getCashFlowType(
                    posting.getSource().getAccountType(),
                    posting.getDestination().getAccountType());
            cashFlowMap.put(type, posting.getVolume().getAmount());
        }

        return WithdrawalCashFlowResult.builder()
                .amount(cashFlowMap.getOrDefault(WithdrawalCashFlowType.AMOUNT, 0L))
                .systemFee(cashFlowMap.getOrDefault(WithdrawalCashFlowType.FEE, 0L))
                .providerFee(cashFlowMap.getOrDefault(WithdrawalCashFlowType.PROVIDER_FEE, 0L))
                .build();
    }

    private WithdrawalCashFlowType getCashFlowType(CashFlowAccount source, CashFlowAccount destination) {
        if (isWalletSenderSettlement(source) && isWalletReceiverDestination(destination)) {
            return WithdrawalCashFlowType.AMOUNT;
        }

        if (isWalletSenderSettlement(source) && isSystemSettlement(destination)) {
            return WithdrawalCashFlowType.FEE;
        }

        if (isSystemSettlement(source) && isProviderSettlement(destination)) {
            return WithdrawalCashFlowType.PROVIDER_FEE;
        }

        if (isMerchantSettlement(source) && isProviderSettlement(destination)) {
            return WithdrawalCashFlowType.REFUND_AMOUNT;
        }

        return WithdrawalCashFlowType.UNKNOWN;
    }

    private boolean isWalletSenderSettlement(CashFlowAccount account) {
        return account.isSetWallet() && account.getWallet() == WalletCashFlowAccount.sender_settlement;
    }

    private boolean isWalletReceiverDestination(CashFlowAccount account) {
        return account.isSetWallet() && account.getWallet() == WalletCashFlowAccount.receiver_destination;
    }

    private boolean isSystemSettlement(CashFlowAccount account) {
        return account.isSetSystem() && account.getSystem() == SystemCashFlowAccount.settlement;
    }

    private boolean isProviderSettlement(CashFlowAccount account) {
        return account.isSetProvider() && account.getProvider() == ProviderCashFlowAccount.settlement;
    }

    private boolean isMerchantSettlement(CashFlowAccount account) {
        return account.isSetMerchant() && account.getMerchant() == MerchantCashFlowAccount.settlement;
    }

    private enum WithdrawalCashFlowType {
        AMOUNT,
        FEE,
        PROVIDER_FEE,
        REFUND_AMOUNT,
        UNKNOWN
    }
}
