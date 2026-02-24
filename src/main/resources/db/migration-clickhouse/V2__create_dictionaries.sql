CREATE NAMED COLLECTION IF NOT EXISTS pg_analytics AS
    host = 'localhost',
    port = 5432,
    user = 'postgres',
    password = 'postgres',
    database = 'analytics',
    schema = 'analytics';

DROP DICTIONARY IF EXISTS analytic.party_dictionary;
DROP DICTIONARY IF EXISTS analytic.shop_dictionary;
DROP DICTIONARY IF EXISTS analytic.category_dictionary;
DROP DICTIONARY IF EXISTS analytic.country_dictionary;
DROP DICTIONARY IF EXISTS analytic.trade_bloc_dictionary;
DROP DICTIONARY IF EXISTS analytic.rate_dictionary;

CREATE DICTIONARY IF NOT EXISTS analytic.party_dictionary (
    id 				 UInt64,
    event_id         String,
    event_time		 String,
    party_id         String,
    created_at       String,
    email            String,
    blocking   	     String,
    blocked_reason   String,
    blocked_since    String,
    unblocked_reason String,
    unblocked_since  String,
    suspension       String,
    suspension_active_since String,
    suspension_suspended_since String,
    revision_id      String,
    revision_changed_at  String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'party'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.shop_dictionary (
	id 				 UInt64,
	event_id         String,
    event_time		 String,
	party_id         String,
	shop_id 	     String,

	category_id      Int32,
	contract_id      String,
	payout_tool_id   String,
	payout_schedule_id Int32,
	created_at         String,
	blocked_reason     String,
	blocked_since      String,
	unblocked_reason   String,
	unblocked_since	   String,
	suspension         String,
	suspension_active_since String,
	suspension_suspended_since String,
	details_name String,
	details_description String,
	location_url String,
	account_currency_code String,
	account_settlement String,
	account_guarantee String,
	account_payout String,

    contractor_id String,
    contractor_type String,
    reg_user_email String,
    legal_entity_type String,
    russian_legal_entity_name String,
    russian_legal_entity_registered_number String,
    russian_legal_entity_inn String,
    russian_legal_entity_actual_address String,
    russian_legal_entity_post_address String,
    russian_legal_entity_representative_position String,
    russian_legal_entity_representative_full_name String,
    russian_legal_entity_representative_document String,
    russian_legal_entity_bank_account String,
    russian_legal_entity_bank_name String,
    russian_legal_entity_bank_post_account String,
    russian_legal_entity_bank_bik String,

    international_legal_entity_name String,
    international_legal_entity_trading_name String,
    international_actual_address String,
    international_legal_entity_registered_address String,
    international_legal_entity_registered_number String,
    international_legal_entity_country_code String,

    private_entity_type String,
    russian_private_entity_first_name String,
    russian_private_entity_second_name String,
    russian_private_entity_middle_name String,
    russian_private_entity_phone_number String,
    russian_private_entity_email String,

    contractor_identification_level String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'shop'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.category_dictionary (
    id 				 UInt64,
    version_id       UInt64,
    category_id      UInt32,
    name             String,
    description      String,
    type             String,
    deleted          String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'category'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.country_dictionary (
    id 				 UInt64,
    version_id       UInt64,
    country_id       UInt32,
    name             String,
    trade_bloc       Array(String),
    deleted          String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'country'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.trade_bloc_dictionary (
    id 				 UInt64,
    version_id       UInt64,
    trade_bloc_id    UInt32,
    name             String,
    description      String,
    deleted          String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'trade_bloc'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);

CREATE DICTIONARY IF NOT EXISTS analytic.rate_dictionary (
    id 				            UInt64,
    event_id 		            String,
    event_time		            String,
    source_symbolic_code		String,
    source_exponent		        UInt32,
    destination_symbolic_code   String,
    destination_exponent        UInt32,
    exchange_rate_rational_p    UInt64,
    exchange_rate_rational_q    UInt64
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
        NAME pg_analytics
        DB 'analytics'
        SCHEMA 'analytics'
        TABLE 'rate'
       ))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360);
