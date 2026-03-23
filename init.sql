-- ==========================================
-- STAGE СЛОЙ
-- ==========================================
CREATE TABLE IF NOT EXISTS stg_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    payload JSONB,
    event_dts TIMESTAMP,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_processed BOOLEAN DEFAULT FALSE
);

-- ==========================================
-- DDS СЛОЙ (Data Vault 2.0)
-- ==========================================

-- Хабы
CREATE TABLE IF NOT EXISTS hub_user (user_hk VARCHAR(32) PRIMARY KEY, user_id INT, load_dts TIMESTAMP, rec_src VARCHAR(50));
CREATE TABLE IF NOT EXISTS hub_content (content_hk VARCHAR(32) PRIMARY KEY, content_id INT, load_dts TIMESTAMP, rec_src VARCHAR(50));
CREATE TABLE IF NOT EXISTS hub_platform (platform_hk VARCHAR(32) PRIMARY KEY, platform_name VARCHAR(50), load_dts TIMESTAMP, rec_src VARCHAR(50));

-- Линки
CREATE TABLE IF NOT EXISTS t_link_user_login (login_hk VARCHAR(32) PRIMARY KEY, user_hk VARCHAR(32), platform_hk VARCHAR(32), event_dts TIMESTAMP, load_dts TIMESTAMP, rec_src VARCHAR(50));
CREATE TABLE IF NOT EXISTS t_link_user_action (action_hk VARCHAR(32) PRIMARY KEY, user_hk VARCHAR(32), content_hk VARCHAR(32), action_type VARCHAR(50), action_value TEXT, event_dts TIMESTAMP, load_dts TIMESTAMP, rec_src VARCHAR(50));

-- Сателлиты (SCD2)
CREATE TABLE IF NOT EXISTS sat_user_profile (user_hk VARCHAR(32), hash_diff VARCHAR(32), subscription_level VARCHAR(20), city VARCHAR(50), valid_from TIMESTAMP, valid_to TIMESTAMP, is_active BOOLEAN, PRIMARY KEY (user_hk, hash_diff));
CREATE TABLE sat_platform (
    platform_hk VARCHAR(32) REFERENCES hub_platform(platform_hk),
    hash_diff VARCHAR(32),
    current_version VARCHAR(20),
    is_stable BOOLEAN,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (platform_hk, hash_diff)
);
