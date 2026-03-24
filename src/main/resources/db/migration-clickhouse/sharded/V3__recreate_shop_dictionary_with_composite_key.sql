DROP DICTIONARY IF EXISTS analytic.shop_dictionary ON CLUSTER '{cluster}';

CREATE DICTIONARY IF NOT EXISTS analytic.shop_dictionary ON CLUSTER '{cluster}' (
    party_id String,
    shop_id String,
    id UInt64,
    event_id UInt64,
    event_time DateTime,
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
PRIMARY KEY party_id, shop_id
SOURCE(POSTGRESQL(
        HOST '<<postgresHost>>'
        PORT <<postgresPort>>
        USER '<<postgresUser>>'
        PASSWORD '<<postgresPassword>>'
        DB '<<postgresDatabase>>'
        SCHEMA '<<postgresSchema>>'
        TABLE 'shop'
       ))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 300 MAX 360);
