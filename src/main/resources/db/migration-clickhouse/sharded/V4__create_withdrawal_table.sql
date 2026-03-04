CREATE TABLE IF NOT EXISTS analytic.events_sink_withdrawal_local (
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
    guaranteeDeposit UInt64,
    systemFee UInt64,
    providerFee UInt64,
    externalFee UInt64,
    currency String,
    status Enum8('pending' = 1, 'succeeded' = 2, 'failed' = 3)
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/<<cluster>>/tables/<<shard>>/{database}/{table}', '<<replica>>')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (eventTimeHour, partyId, status, currency, providerId, terminal, withdrawalId, sequenceId);

CREATE TABLE IF NOT EXISTS analytic.events_sink_withdrawal AS analytic.events_sink_withdrawal_local
ENGINE = Distributed('<<cluster>>', analytic, events_sink_withdrawal_local, cityHash64(timestamp, partyId));
