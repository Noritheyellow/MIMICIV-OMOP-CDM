-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate staging tables for cdm dimension tables
-- 
-- Dependencies: run first after DDL
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- transfers.stay_id - does not exist in Demo, but is described in the online Documentation
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- src_patients
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.src_patients AS
SELECT 
    subject_id                          AS subject_id,
    anchor_year                         AS anchor_year,
    anchor_age                          AS anchor_age,
    anchor_year_group                   AS anchor_year_group,
    gender                              AS gender,
    --
    'patients'                          AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id
    ))                                  AS trace_id
FROM
    `@source_project`.@core_dataset.patients
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.src_admissions AS
SELECT
    hadm_id                             AS hadm_id, -- PK
    subject_id                          AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    race                                AS ethnicity, -- MIMIC IV 2.0 change, field race replaced field ethnicity
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id
    ))                                  AS trace_id
FROM
    `@source_project`.@core_dataset.admissions
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.src_transfers AS
SELECT
    transfer_id                         AS transfer_id,
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    careunit                            AS careunit,
    intime                              AS intime,
    outtime                             AS outtime,
    eventtype                           AS eventtype,
    --
    'transfers'                         AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        transfer_id AS transfer_id
    ))                                  AS trace_id
FROM
    `@source_project`.@core_dataset.transfers
;

-- -------------------------------------------------------------------
-- 2024.09.19. David Hwang.
-- Modify above query fit into our project 'AEGIS'.
-- -------------------------------------------------------------------

-- Explanation:
-- FARM_FINGERPRINT(GENERATE_UUID()) in BigQuery is replaced with md5(uuid_generate_v4()::text) in PostgreSQL. 
-- If you want a different hash function, you can change md5 to something else (like sha256).
-- 
-- TO_JSON_STRING(STRUCT(...)) is replaced with row_to_json(...)::text, converting the ROW into JSON format.
-- 
-- GENERATE_UUID() in BigQuery is equivalent to uuid_generate_v4() in PostgreSQL, 
-- but you'll need the uuid-ossp extension enabled in your PostgreSQL database. 
-- If it's not enabled, run CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; before executing the query.

-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- Although we can install extension `uuid-ossp`, using a function `md5(gen_random_uuid())` is also available.
-- https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-uuid/


DROP TABLE IF EXISTS mimic_omop_cdm.src_patients;
CREATE TABLE mimic_omop_cdm.src_patients AS
SELECT 
    subject_id                              AS subject_id,
    anchor_year                             AS anchor_year,
    anchor_age                              AS anchor_age,
    anchor_year_group                       AS anchor_year_group,
    gender                                  AS gender,
    --
    'patients'                              AS load_table_id,
    md5(gen_random_uuid()::TEXT)   		    AS load_row_id,
    row_to_json(ROW(subject_id))::text      AS trace_id 
FROM
    mimiciv_hosp.patients
;


DROP TABLE IF EXISTS mimic_omop_cdm.src_admissions;
CREATE TABLE mimic_omop_cdm.src_admissions AS
SELECT
    hadm_id                             AS hadm_id, -- PK
    subject_id                          AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    race                                AS ethnicity, -- MIMIC IV 2.0 change, field race replaced field ethnicity
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    md5(gen_random_uuid()::TEXT)   		AS load_row_id,
    row_to_json(ROW(
        subject_id,
        hadm_id
    ))::TEXT                            AS trace_id
FROM
    mimiciv_hosp.admissions
;


DROP TABLE IF EXISTS mimic_omop_cdm.src_transfers;
CREATE TABLE mimic_omop_cdm.src_transfers AS
SELECT
    transfer_id                         AS transfer_id,
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    careunit                            AS careunit,
    intime                              AS intime,
    outtime                             AS outtime,
    eventtype                           AS eventtype,
    --
    'transfers'                         AS load_table_id,
    md5(gen_random_uuid()::TEXT)        AS load_row_id,
    row_to_json(ROW(
        subject_id,
        hadm_id,
        transfer_id
    ))::TEXT                            AS trace_id
FROM
    mimiciv_hosp.transfers
;