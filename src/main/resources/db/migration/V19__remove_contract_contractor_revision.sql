DROP INDEX IF EXISTS analytics.shop_party_id_contract_id_idx;
DROP INDEX IF EXISTS analytics.contract_uidx;
DROP INDEX IF EXISTS analytics.contractor_id_idx;

ALTER TABLE analytics.shop
    DROP COLUMN IF EXISTS contract_id,
    DROP COLUMN IF EXISTS contractor_id,
    DROP COLUMN IF EXISTS contractor_type,
    DROP COLUMN IF EXISTS reg_user_email,
    DROP COLUMN IF EXISTS legal_entity_type,
    DROP COLUMN IF EXISTS russian_legal_entity_name,
    DROP COLUMN IF EXISTS russian_legal_entity_registered_number,
    DROP COLUMN IF EXISTS russian_legal_entity_inn,
    DROP COLUMN IF EXISTS russian_legal_entity_actual_address,
    DROP COLUMN IF EXISTS russian_legal_entity_post_address,
    DROP COLUMN IF EXISTS russian_legal_entity_representative_position,
    DROP COLUMN IF EXISTS russian_legal_entity_representative_full_name,
    DROP COLUMN IF EXISTS russian_legal_entity_representative_document,
    DROP COLUMN IF EXISTS russian_legal_entity_bank_account,
    DROP COLUMN IF EXISTS russian_legal_entity_bank_name,
    DROP COLUMN IF EXISTS russian_legal_entity_bank_post_account,
    DROP COLUMN IF EXISTS russian_legal_entity_bank_bik,
    DROP COLUMN IF EXISTS international_legal_entity_name,
    DROP COLUMN IF EXISTS international_legal_entity_trading_name,
    DROP COLUMN IF EXISTS international_legal_entity_registered_address,
    DROP COLUMN IF EXISTS international_legal_entity_registered_number,
    DROP COLUMN IF EXISTS international_actual_address,
    DROP COLUMN IF EXISTS private_entity_type,
    DROP COLUMN IF EXISTS russian_private_entity_first_name,
    DROP COLUMN IF EXISTS russian_private_entity_second_name,
    DROP COLUMN IF EXISTS russian_private_entity_middle_name,
    DROP COLUMN IF EXISTS russian_private_entity_phone_number,
    DROP COLUMN IF EXISTS russian_private_entity_email,
    DROP COLUMN IF EXISTS contractor_identification_level;

ALTER TABLE analytics.party
    DROP COLUMN IF EXISTS revision_id,
    DROP COLUMN IF EXISTS revision_changed_at;

DROP TABLE IF EXISTS analytics.contract;
DROP TABLE IF EXISTS analytics.contractor;

DROP TYPE IF EXISTS analytics.contractor_type;
DROP TYPE IF EXISTS analytics.contractor_identification_lvl;
