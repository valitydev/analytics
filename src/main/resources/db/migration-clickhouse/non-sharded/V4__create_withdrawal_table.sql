CREATE TABLE IF NOT EXISTS analytic.events_sink_withdrawal (
    timestamp Date,
    eventTime UInt64,
    eventTimeHour UInt64,
    partyId String,
    withdrawalId String,
    sequenceId UInt64,
    withdrawalTime UInt64,
    walletId String,
    destinationId String,
    providerId String,
    terminal String,
    amount UInt64,
    systemFee UInt64,
    providerFee UInt64,
    currency String,
    status Enum8('pending' = 1, 'succeeded' = 2, 'failed' = 3)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (eventTimeHour, partyId, walletId, status, currency, providerId, terminal, withdrawalId, sequenceId);
