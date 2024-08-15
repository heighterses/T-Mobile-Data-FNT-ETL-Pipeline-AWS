CREATE ROLE readwrite;
GRANT CONNECT ON DATABASE dev_fnt_rds_onboarding_service TO readwrite;
GRANT USAGE ON SCHEMA dev_fnt_rds_onboarding_service.fnt TO readwrite;
GRANT USAGE, CREATE ON SCHEMA dev_fnt_rds_onboarding_service.fnt TO readwrite;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dev_fnt_rds_onboarding_service.fnt TO readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA fnt GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO readwrite;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA dev_fnt_rds_onboarding_service.fnt TO readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA dev_fnt_rds_onboarding_service.fnt GRANT USAGE ON SEQUENCES TO readwrite;