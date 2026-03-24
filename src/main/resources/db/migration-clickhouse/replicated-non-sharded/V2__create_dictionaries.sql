DROP DICTIONARY IF EXISTS analytic.party_dictionary ON CLUSTER '{cluster}';
DROP DICTIONARY IF EXISTS analytic.shop_dictionary ON CLUSTER '{cluster}';
DROP DICTIONARY IF EXISTS analytic.category_dictionary ON CLUSTER '{cluster}';
DROP DICTIONARY IF EXISTS analytic.country_dictionary ON CLUSTER '{cluster}';
DROP DICTIONARY IF EXISTS analytic.trade_bloc_dictionary ON CLUSTER '{cluster}';
DROP DICTIONARY IF EXISTS analytic.rate_dictionary ON CLUSTER '{cluster}';

CREATE DICTIONARY IF NOT EXISTS analytic.party_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    event_id UInt64,
    event_time DateTime,
    party_id String,
    created_at DateTime,
    email String,
    blocking String,
    blocked_reason String,
    blocked_since DateTime,
    unblocked_reason String,
    unblocked_since DateTime,
    suspension String,
    suspension_active_since DateTime,
    suspension_suspended_since DateTime,
    version_id UInt64,
    changed_by_id String,
    changed_by_email String,
    changed_by_name String,
    deleted Bool
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'party'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.shop_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    event_id UInt64,
    event_time DateTime,
    party_id String,
    shop_id String,
    category_id Int32,
    created_at DateTime,
    blocking String,
    blocked_reason String,
    blocked_since DateTime,
    unblocked_reason String,
    unblocked_since DateTime,
    suspension String,
    suspension_active_since DateTime,
    suspension_suspended_since DateTime,
    details_name String,
    details_description String,
    location_url String,
    account_currency_code String,
    account_settlement String,
    account_guarantee String,
    international_legal_entity_country_code String,
    version_id UInt64,
    changed_by_id String,
    changed_by_email String,
    changed_by_name String,
    deleted Bool
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'shop'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.category_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    version_id UInt64,
    category_id Int32,
    name String,
    description String,
    type String,
    deleted Bool,
    changed_by_id String,
    changed_by_email String,
    changed_by_name String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'category'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.country_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    version_id UInt64,
    country_id String,
    name String,
    trade_bloc Array(String),
    deleted Bool,
    changed_by_id String,
    changed_by_email String,
    changed_by_name String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'country'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.trade_bloc_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    version_id UInt64,
    trade_bloc_id String,
    name String,
    description String,
    deleted Bool,
    changed_by_id String,
    changed_by_email String,
    changed_by_name String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'trade_bloc'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.rate_dictionary ON CLUSTER '{cluster}' (
    id UInt64,
    event_id String,
    event_time String,
    source_symbolic_code String,
    source_exponent UInt32,
    destination_symbolic_code String,
    destination_exponent UInt32,
    exchange_rate_rational_p UInt64,
    exchange_rate_rational_q UInt64
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'rate'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);
