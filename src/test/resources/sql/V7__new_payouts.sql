DROP TABLE IF EXISTS analytic.events_sink_payout;

CREATE TABLE analytic.events_sink_payout
(
    payoutId                                  String,
    status                                    Enum8('unpaid' = 1, 'paid' = 2, 'cancelled' = 3, 'confirmed' = 4),
    payoutToolId                              String,
    statusCancelledDetails                    String,
    isCancelledAfterBeingPaid                 UInt8,

    timestamp                                 Date,
    eventTime                                 UInt64,
    eventTimeHour                             UInt64,
    payoutTime                                UInt64,

    shopId                                    String,
    partyId                                   String,

    amount                                    UInt64,
    fee                                       UInt64,
    currency                                  String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, status, payoutId, currency)
