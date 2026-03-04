package dev.vality.analytics.dao.repository.postgres;

import dev.vality.analytics.dao.model.WithdrawalStateSnapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresWithdrawalStateRepository {

    private static final String UPSERT = "INSERT INTO analytics.withdrawal_state " +
            "(withdrawal_id, party_id, wallet_id, destination_id, currency, requested_amount, amount, " +
            "system_fee, " +
            "provider_fee, external_fee, withdrawal_created_at, provider_id, terminal, " +
            "last_sequence_id, " +
            "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (withdrawal_id) DO UPDATE SET " +
            "party_id = EXCLUDED.party_id, " +
            "wallet_id = EXCLUDED.wallet_id, " +
            "destination_id = EXCLUDED.destination_id, " +
            "currency = EXCLUDED.currency, " +
            "requested_amount = EXCLUDED.requested_amount, " +
            "amount = EXCLUDED.amount, " +
            "system_fee = EXCLUDED.system_fee, " +
            "provider_fee = EXCLUDED.provider_fee, " +
            "external_fee = EXCLUDED.external_fee, " +
            "withdrawal_created_at = EXCLUDED.withdrawal_created_at, " +
            "provider_id = EXCLUDED.provider_id, " +
            "terminal = EXCLUDED.terminal, " +
            "last_sequence_id = EXCLUDED.last_sequence_id, " +
            "updated_at = EXCLUDED.updated_at";
    private static final String SELECT = "SELECT withdrawal_id, party_id, wallet_id, destination_id, currency, " +
            "requested_amount, amount, system_fee, provider_fee, external_fee, withdrawal_created_at, " +
            "provider_id, terminal, last_sequence_id, updated_at " +
            "FROM analytics.withdrawal_state " +
            "WHERE withdrawal_id = ?";

    private final JdbcTemplate postgresJdbcTemplate;

    public Optional<WithdrawalStateSnapshot> findByWithdrawalId(String withdrawalId) {
        return postgresJdbcTemplate.query(
                        SELECT,
                        (resultSet, rowNum) -> map(resultSet),
                        withdrawalId)
                .stream()
                .findFirst();
    }

    public void upsert(WithdrawalStateSnapshot snapshot) {
        postgresJdbcTemplate.update(
                UPSERT,
                snapshot.getWithdrawalId(),
                snapshot.getPartyId(),
                snapshot.getWalletId(),
                snapshot.getDestinationId(),
                snapshot.getCurrency(),
                snapshot.getRequestedAmount(),
                snapshot.getAmount(),
                snapshot.getSystemFee(),
                snapshot.getProviderFee(),
                snapshot.getExternalFee(),
                snapshot.getWithdrawalCreatedAt(),
                snapshot.getProviderId(),
                snapshot.getTerminal(),
                snapshot.getLastSequenceId(),
                snapshot.getUpdatedAt());
        log.debug("Upserted withdrawal state, withdrawalId={}, sequenceId={}",
                snapshot.getWithdrawalId(), snapshot.getLastSequenceId());
    }

    private WithdrawalStateSnapshot map(ResultSet resultSet) throws SQLException {
        return WithdrawalStateSnapshot.builder()
                .withdrawalId(resultSet.getString("withdrawal_id"))
                .partyId(resultSet.getString("party_id"))
                .walletId(resultSet.getString("wallet_id"))
                .destinationId(resultSet.getString("destination_id"))
                .currency(resultSet.getString("currency"))
                .requestedAmount((Long) resultSet.getObject("requested_amount"))
                .amount((Long) resultSet.getObject("amount"))
                .systemFee((Long) resultSet.getObject("system_fee"))
                .providerFee((Long) resultSet.getObject("provider_fee"))
                .externalFee((Long) resultSet.getObject("external_fee"))
                .withdrawalCreatedAt(resultSet.getTimestamp("withdrawal_created_at") != null
                        ? resultSet.getTimestamp("withdrawal_created_at").toLocalDateTime()
                        : null)
                .providerId(resultSet.getString("provider_id"))
                .terminal(resultSet.getString("terminal"))
                .lastSequenceId(resultSet.getLong("last_sequence_id"))
                .updatedAt(resultSet.getTimestamp("updated_at").toLocalDateTime())
                .build();
    }
}
