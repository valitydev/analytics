package dev.vality.analytics.utils;

import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.cashflow.CashFlowAccount;
import dev.vality.fistful.cashflow.ExternalCashFlowAccount;
import dev.vality.fistful.cashflow.FinalCashFlow;
import dev.vality.fistful.cashflow.FinalCashFlowAccount;
import dev.vality.fistful.cashflow.FinalCashFlowPosting;
import dev.vality.fistful.cashflow.ProviderCashFlowAccount;
import dev.vality.fistful.cashflow.SystemCashFlowAccount;
import dev.vality.fistful.cashflow.WalletCashFlowAccount;
import dev.vality.fistful.transfer.Transfer;
import dev.vality.fistful.withdrawal.Change;
import dev.vality.fistful.withdrawal.CreatedChange;
import dev.vality.fistful.withdrawal.Route;
import dev.vality.fistful.withdrawal.RouteChange;
import dev.vality.fistful.withdrawal.StatusChange;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.fistful.withdrawal.TransferChange;
import dev.vality.fistful.withdrawal.Withdrawal;
import dev.vality.fistful.withdrawal.status.Pending;
import dev.vality.fistful.withdrawal.status.Status;
import dev.vality.fistful.withdrawal.status.Succeeded;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.geck.serializer.Geck;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import org.apache.thrift.TBase;

import java.time.Instant;
import java.util.List;

public final class WithdrawalEventTestUtils {

    public static final String WITHDRAWAL_ID = "withdrawal-1";
    public static final String PARTY_ID = "party-1";
    public static final String WALLET_ID = "wallet-1";
    public static final String DESTINATION_ID = "destination-1";
    public static final String CURRENCY = "RUB";
    public static final String CREATED_AT = "2024-01-10T10:15:30Z";
    public static final String OCCURRED_AT = "2024-01-10T10:15:35Z";

    private WithdrawalEventTestUtils() {
    }

    public static TimestampedChange createdChange(long requestedAmount, Integer providerId, Integer terminalId) {
        Withdrawal withdrawal = new Withdrawal()
                .setId(WITHDRAWAL_ID)
                .setPartyId(PARTY_ID)
                .setWalletId(WALLET_ID)
                .setDestinationId(DESTINATION_ID)
                .setBody(new Cash().setAmount(requestedAmount).setCurrency(new CurrencyRef(CURRENCY)))
                .setCreatedAt(CREATED_AT)
                .setDomainRevision(1L);

        if (providerId != null) {
            Route route = new Route().setProviderId(providerId);
            if (terminalId != null) {
                route.setTerminalId(terminalId);
            }
            withdrawal.setRoute(route);
        }

        return new TimestampedChange()
                .setOccuredAt(OCCURRED_AT)
                .setChange(Change.created(new CreatedChange().setWithdrawal(withdrawal)));
    }

    public static TimestampedChange routeChange(int providerId, Integer terminalId) {
        Route route = new Route().setProviderId(providerId);
        if (terminalId != null) {
            route.setTerminalId(terminalId);
        }

        return new TimestampedChange()
                .setOccuredAt(OCCURRED_AT)
                .setChange(Change.route(new RouteChange().setRoute(route)));
    }

    public static TimestampedChange transferCreatedChange(
            long amount,
            long systemFee,
            long providerFee) {
        FinalCashFlow cashFlow = new FinalCashFlow().setPostings(List.of(
                walletSenderSettlementToReceiverDestination(amount),
                walletSenderSettlementToSystem(systemFee),
                systemToProvider(providerFee),
                unrelatedPosting(999L)));

        Transfer transfer = new Transfer()
                .setId("transfer-1")
                .setCashflow(cashFlow);

        return new TimestampedChange()
                .setOccuredAt(OCCURRED_AT)
                .setChange(Change.transfer(new TransferChange().setPayload(
                        dev.vality.fistful.transfer.Change.created(
                                new dev.vality.fistful.transfer.CreatedChange().setTransfer(transfer)))));
    }

    public static TimestampedChange pendingStatusChange() {
        return new TimestampedChange()
                .setOccuredAt(OCCURRED_AT)
                .setChange(Change.status_changed(new StatusChange().setStatus(Status.pending(new Pending()))));
    }

    public static TimestampedChange succeededStatusChange() {
        return new TimestampedChange()
                .setOccuredAt(OCCURRED_AT)
                .setChange(Change.status_changed(new StatusChange().setStatus(Status.succeeded(new Succeeded()))));
    }

    public static MachineEvent machineEvent(long eventId, TimestampedChange timestampedChange) {
        return new MachineEvent()
                .setSourceNs("withdrawal")
                .setSourceId(WITHDRAWAL_ID)
                .setEventId(eventId)
                .setCreatedAt(TypeUtil.temporalToString(Instant.parse(OCCURRED_AT)))
                .setData(dev.vality.machinegun.msgpack.Value.bin(Geck.toMsgPack(timestampedChange)));
    }

    public static SinkEvent sinkEvent(long eventId, TimestampedChange timestampedChange) {
        return SinkEvent.event(machineEvent(eventId, timestampedChange));
    }

    public static List<TBase<?, ?>> fullSuccessFlow() {
        return List.of(
                sinkEvent(1L, createdChange(1500L, null, null)),
                sinkEvent(2L, routeChange(42, 24)),
                sinkEvent(3L, transferCreatedChange(1200L, 100L, 20L)),
                sinkEvent(4L, succeededStatusChange()));
    }

    public static FinalCashFlowPosting walletSenderSettlementToReceiverDestination(long amount) {
        return posting(
                CashFlowAccount.wallet(WalletCashFlowAccount.sender_settlement),
                CashFlowAccount.wallet(WalletCashFlowAccount.receiver_destination),
                amount);
    }

    public static FinalCashFlowPosting walletSenderSettlementToSystem(long amount) {
        return posting(
                CashFlowAccount.wallet(WalletCashFlowAccount.sender_settlement),
                CashFlowAccount.system(SystemCashFlowAccount.settlement),
                amount);
    }

    public static FinalCashFlowPosting systemToProvider(long amount) {
        return posting(
                CashFlowAccount.system(SystemCashFlowAccount.settlement),
                CashFlowAccount.provider(ProviderCashFlowAccount.settlement),
                amount);
    }

    public static FinalCashFlowPosting systemToExternal(long amount) {
        return posting(
                CashFlowAccount.system(SystemCashFlowAccount.settlement),
                CashFlowAccount.external(ExternalCashFlowAccount.outcome),
                amount);
    }

    public static FinalCashFlowPosting unrelatedPosting(long amount) {
        return posting(
                CashFlowAccount.provider(ProviderCashFlowAccount.settlement),
                CashFlowAccount.system(SystemCashFlowAccount.settlement),
                amount);
    }

    private static FinalCashFlowPosting posting(CashFlowAccount source, CashFlowAccount destination, long amount) {
        return new FinalCashFlowPosting()
                .setSource(new FinalCashFlowAccount().setAccountType(source))
                .setDestination(new FinalCashFlowAccount().setAccountType(destination))
                .setVolume(new Cash().setAmount(amount).setCurrency(new CurrencyRef(CURRENCY)));
    }
}
