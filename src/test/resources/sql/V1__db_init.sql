CREATE DATABASE IF NOT EXISTS analytic;

DROP TABLE IF EXISTS analytic.events_sink;

create table analytic.events_sink
(
    timestamp     Date,
    eventTime     UInt64,
    eventTimeHour UInt64,

    partyId       String,
    shopId        String,

    email         String,

    amount        UInt64,
    currency      String,

    providerName      String,
    status        String,
    errorReason   String,

    invoiceId     String,
    paymentId     String,
    sequenceId    UInt64,

    ip            String,
    bin           String,
    maskedPan     String,
    paymentTool   String,

    sign          Int8

) ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, paymentTool, status, currency, providerName, invoiceId, paymentId, sequenceId);