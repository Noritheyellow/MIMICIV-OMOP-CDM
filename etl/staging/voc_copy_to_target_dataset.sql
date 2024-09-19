-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Copy vocabulary tables from the master vocab dataset
-- (to apply custom mapping here?)
-- -------------------------------------------------------------------

-- check
-- SELECT 'VOC', COUNT(*) FROM `@voc_project`.@voc_dataset.concept
-- UNION ALL
-- SELECT 'TARGET', COUNT(*) FROM `@etl_project`.@etl_dataset.voc_concept
-- ;

-- affected by custom mapping

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_concept AS
SELECT * FROM `@voc_project`.@voc_dataset.concept
;

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_concept_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_relationship
;

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_vocabulary AS
SELECT * FROM `@voc_project`.@voc_dataset.vocabulary
;

-- not affected by custom mapping

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_domain AS
SELECT * FROM `@voc_project`.@voc_dataset.domain
;
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_concept_class AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_class
;
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.relationship
;
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_concept_synonym AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_synonym
;
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_concept_ancestor AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_ancestor
;
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.voc_drug_strength AS
SELECT * FROM `@voc_project`.@voc_dataset.drug_strength
;


-- -------------------------------------------------------------------
-- 2024.09.20. David Hwang.
-- Modify above query fit into our project 'AEGIS'.
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS mimic_omop_cdm.voc_concept;
CREATE TABLE mimic_omop_cdm.voc_concept AS
SELECT * FROM standardized_vocabularies.concept
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_concept_relationship;
CREATE TABLE mimic_omop_cdm.voc_concept_relationship AS
SELECT * FROM standardized_vocabularies.concept_relationship
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_vocabulary;
CREATE TABLE mimic_omop_cdm.voc_vocabulary AS
SELECT * FROM standardized_vocabularies.vocabulary
;

-- not affected by custom mapping

DROP TABLE IF EXISTS mimic_omop_cdm.voc_domain;
CREATE TABLE mimic_omop_cdm.voc_domain AS
SELECT * FROM standardized_vocabularies."DOMAIN"
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_concept_class;
CREATE TABLE mimic_omop_cdm.voc_concept_class AS
SELECT * FROM standardized_vocabularies.concept_class
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_relationship;
CREATE TABLE mimic_omop_cdm.voc_relationship AS
SELECT * FROM standardized_vocabularies.relationship
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_concept_synonym;
CREATE TABLE mimic_omop_cdm.voc_concept_synonym AS
SELECT * FROM standardized_vocabularies.concept_synonym
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_concept_ancestor;
CREATE TABLE mimic_omop_cdm.voc_concept_ancestor AS
SELECT * FROM standardized_vocabularies.concept_ancestor
;

DROP TABLE IF EXISTS mimic_omop_cdm.voc_drug_strength;
CREATE TABLE mimic_omop_cdm.voc_drug_strength AS
SELECT * FROM standardized_vocabularies.drug_strength
;
