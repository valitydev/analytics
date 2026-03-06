package dev.vality.analytics.listener.handler.withdrawal;

import dev.vality.analytics.computer.WithdrawalCashFlowComputer;
import dev.vality.analytics.dao.model.WithdrawalRow;
import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import dev.vality.analytics.dao.repository.clickhouse.ClickHouseWithdrawalRepository;
import dev.vality.analytics.dao.repository.postgres.PostgresWithdrawalStateRepository;
import dev.vality.analytics.listener.mapper.withdrawal.WithdrawalCreatedMapper;
import dev.vality.analytics.listener.mapper.withdrawal.WithdrawalRouteMapper;
import dev.vality.analytics.listener.mapper.withdrawal.WithdrawalStatusMapper;
import dev.vality.analytics.listener.mapper.withdrawal.WithdrawalTransferMapper;
import dev.vality.analytics.utils.WithdrawalEventTestUtils;
import dev.vality.fistful.withdrawal.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class WithdrawalEventHandlerTest {

    @Mock
    private MachineEventParser<TimestampedChange> machineEventParser;
    @Mock
    private PostgresWithdrawalStateRepository postgresWithdrawalStateRepository;
    @Mock
    private ClickHouseWithdrawalRepository clickHouseWithdrawalRepository;

    private WithdrawalEventHandler withdrawalEventHandler;
    private Map<String, WithdrawalStateSnapshot> store;

    @BeforeEach
    public void setUp() {
        withdrawalEventHandler = new WithdrawalEventHandler(
                machineEventParser,
                postgresWithdrawalStateRepository,
                clickHouseWithdrawalRepository,
                List.of(
                        new WithdrawalCreatedMapper(),
                        new WithdrawalRouteMapper(),
                        new WithdrawalTransferMapper(new WithdrawalCashFlowComputer()),
                        new WithdrawalStatusMapper()));
        store = new HashMap<>();

        when(postgresWithdrawalStateRepository.findByWithdrawalId(anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(store.get(invocation.getArgument(0))));
        doAnswer(invocation -> {
            WithdrawalStateSnapshot snapshot = invocation.getArgument(0);
            store.put(snapshot.getWithdrawalId(), snapshot);
            return null;
        }).when(postgresWithdrawalStateRepository).upsert(any(WithdrawalStateSnapshot.class));
    }

    @Test
    public void createdShouldInitializeReducerState() {
        MachineEvent machineEvent = WithdrawalEventTestUtils.machineEvent(1L,
                WithdrawalEventTestUtils.createdChange(1500L, 42, 24));
        when(machineEventParser.parse(machineEvent))
                .thenReturn(WithdrawalEventTestUtils.createdChange(1500L, 42, 24));

        withdrawalEventHandler.handle(List.of(machineEvent));

        WithdrawalStateSnapshot snapshot = store.get(WithdrawalEventTestUtils.WITHDRAWAL_ID);
        assertEquals(WithdrawalEventTestUtils.PARTY_ID, snapshot.getPartyId());
        assertEquals(WithdrawalEventTestUtils.WALLET_ID, snapshot.getWalletId());
        assertEquals(WithdrawalEventTestUtils.DESTINATION_ID, snapshot.getDestinationId());
        assertEquals(WithdrawalEventTestUtils.CURRENCY, snapshot.getCurrency());
        assertEquals(1500L, snapshot.getRequestedAmount());
        assertEquals("42", snapshot.getProviderId());
        assertEquals("24", snapshot.getTerminal());
    }

    @Test
    public void routeShouldUpdateProviderAndTerminal() {
        store.put(WithdrawalEventTestUtils.WITHDRAWAL_ID, WithdrawalStateSnapshot.builder()
                .withdrawalId(WithdrawalEventTestUtils.WITHDRAWAL_ID)
                .partyId(WithdrawalEventTestUtils.PARTY_ID)
                .lastSequenceId(1L)
                .build());

        MachineEvent machineEvent = WithdrawalEventTestUtils.machineEvent(
                2L,
                WithdrawalEventTestUtils.routeChange(77, 55));
        when(machineEventParser.parse(machineEvent)).thenReturn(WithdrawalEventTestUtils.routeChange(77, 55));

        withdrawalEventHandler.handle(List.of(machineEvent));

        WithdrawalStateSnapshot snapshot = store.get(WithdrawalEventTestUtils.WITHDRAWAL_ID);
        assertEquals("77", snapshot.getProviderId());
        assertEquals("55", snapshot.getTerminal());
        assertEquals(2L, snapshot.getLastSequenceId());
    }

    @Test
    public void transferCreatedShouldUpdateMonetaryFields() {
        store.put(WithdrawalEventTestUtils.WITHDRAWAL_ID, WithdrawalStateSnapshot.builder()
                .withdrawalId(WithdrawalEventTestUtils.WITHDRAWAL_ID)
                .partyId(WithdrawalEventTestUtils.PARTY_ID)
                .lastSequenceId(1L)
                .build());

        MachineEvent machineEvent = WithdrawalEventTestUtils.machineEvent(3L,
                WithdrawalEventTestUtils.transferCreatedChange(1200L, 100L, 20L));
        when(machineEventParser.parse(machineEvent))
                .thenReturn(WithdrawalEventTestUtils.transferCreatedChange(1200L, 100L, 20L));

        withdrawalEventHandler.handle(List.of(machineEvent));

        WithdrawalStateSnapshot snapshot = store.get(WithdrawalEventTestUtils.WITHDRAWAL_ID);
        assertEquals(1200L, snapshot.getAmount());
        assertEquals(100L, snapshot.getSystemFee());
        assertEquals(20L, snapshot.getProviderFee());
    }

    @Test
    public void statusChangedShouldWriteSnapshotRowUsingCurrentState() {
        store.put(WithdrawalEventTestUtils.WITHDRAWAL_ID, WithdrawalStateSnapshot.builder()
                .withdrawalId(WithdrawalEventTestUtils.WITHDRAWAL_ID)
                .partyId(WithdrawalEventTestUtils.PARTY_ID)
                .walletId(WithdrawalEventTestUtils.WALLET_ID)
                .destinationId(WithdrawalEventTestUtils.DESTINATION_ID)
                .currency(WithdrawalEventTestUtils.CURRENCY)
                .withdrawalCreatedAt(java.time.LocalDateTime.parse("2024-01-10T10:15:30"))
                .providerId("42")
                .terminal("24")
                .amount(1200L)
                .systemFee(100L)
                .providerFee(20L)
                .lastSequenceId(3L)
                .build());

        MachineEvent machineEvent = WithdrawalEventTestUtils.machineEvent(
                4L,
                WithdrawalEventTestUtils.succeededStatusChange());
        when(machineEventParser.parse(machineEvent)).thenReturn(WithdrawalEventTestUtils.succeededStatusChange());

        withdrawalEventHandler.handle(List.of(machineEvent));

        ArgumentCaptor<List<WithdrawalRow>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(clickHouseWithdrawalRepository).insertBatch(rowsCaptor.capture());
        List<WithdrawalRow> rows = rowsCaptor.getValue();
        assertThat(rows, hasSize(1));
        WithdrawalRow row = rows.get(0);
        assertThat(row.getPartyId(), is(WithdrawalEventTestUtils.PARTY_ID));
        assertThat(row.getCurrency(), is(WithdrawalEventTestUtils.CURRENCY));
        assertThat(row.getProviderId(), is("42"));
        assertThat(row.getTerminal(), is("24"));
        assertThat(row.getAmount(), is(1200L));
        assertThat(row.getSystemFee(), is(100L));
        assertThat(row.getProviderFee(), is(20L));
        assertThat(row.getStatus().name(), is("succeeded"));
    }

    @Test
    public void statusChangedShouldFallbackToRequestedAmountWhenTransferMissing() {
        store.put(WithdrawalEventTestUtils.WITHDRAWAL_ID, WithdrawalStateSnapshot.builder()
                .withdrawalId(WithdrawalEventTestUtils.WITHDRAWAL_ID)
                .partyId(WithdrawalEventTestUtils.PARTY_ID)
                .walletId(WithdrawalEventTestUtils.WALLET_ID)
                .destinationId(WithdrawalEventTestUtils.DESTINATION_ID)
                .currency(WithdrawalEventTestUtils.CURRENCY)
                .withdrawalCreatedAt(java.time.LocalDateTime.parse("2024-01-10T10:15:30"))
                .requestedAmount(1500L)
                .lastSequenceId(1L)
                .build());

        MachineEvent machineEvent = WithdrawalEventTestUtils.machineEvent(
                2L,
                WithdrawalEventTestUtils.pendingStatusChange());
        when(machineEventParser.parse(machineEvent)).thenReturn(WithdrawalEventTestUtils.pendingStatusChange());

        withdrawalEventHandler.handle(List.of(machineEvent));

        ArgumentCaptor<List<WithdrawalRow>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(clickHouseWithdrawalRepository).insertBatch(rowsCaptor.capture());
        WithdrawalRow row = rowsCaptor.getValue().get(0);
        assertThat(row.getAmount(), is(1500L));
        assertThat(row.getSystemFee(), is(0L));
        assertThat(row.getProviderFee(), is(0L));
        assertThat(row.getStatus().name(), is("pending"));
    }
}
