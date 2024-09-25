-- -------------------------------------------------------------------
-- 2024.09.25. David Hwang.
-- mimiciv.d_labitems의 표준 어휘 사전
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS mimic_omop_cdm.david_labitems;
CREATE TABLE mimic_omop_cdm.david_labitems AS
WITH stg1 AS (
SELECT sdl.*
		 , CASE WHEN sdl.loinc_code  IS NOT NULL
		 				THEN 'LOINC'
		 				ELSE 'mimiciv_meas_lab_loinc'
		   END 																		AS source_vocabulary_id
	FROM mimic_omop_cdm.src_d_labitems sdl
)
, stg2 AS (
SELECT sdl.itemid 					AS source_concept_code
		 , sdl."label"
		 , sdl.fluid
		 , sdl.category
		 , vc.concept_name			AS source_concept_name
		 , vc.domain_id					AS source_domain_id
		 , vc.concept_class_id	AS source_concept_class_id
		 , vc.concept_id 				AS source_concept_id
	FROM stg1 sdl
	LEFT JOIN mimic_omop_cdm.voc_concept vc 
		ON sdl.itemid::TEXT = vc.concept_code
	 AND sdl.source_vocabulary_id = vc.vocabulary_id
)
, stg3 AS (
SELECT sdl.*
		 , vcr.relationship_id
		 , vcr.concept_id_2 AS target_concept_id
	FROM stg2 sdl
	LEFT JOIN mimic_omop_cdm.voc_concept_relationship vcr 
		ON sdl.source_concept_id = vcr.concept_id_1 
	 AND vcr.relationship_id = 'Maps to'
)
, stg4 AS (
SELECT sdl.*
		 , vc.concept_name 	AS target_concept_name
		 , vc.vocabulary_id AS target_vocabulary_id
		 , vc.concept_code 	AS target_concept_code
		 , vc.domain_id 		AS target_domain_id
		 , vc.standard_concept AS target_standard_concept
	FROM stg3 sdl
	LEFT JOIN mimic_omop_cdm.voc_concept vc 
		ON sdl.target_concept_id = vc.concept_id 
	 AND vc.standard_concept = 'S'
	 AND vc.invalid_reason IN (NULL, '')
 WHERE sdl.source_concept_id IS NOT NULL
)
SELECT *
	FROM stg4
; 

-- 이렇게 mimiciv.d_labitems의 lab item의 표준을 알고 싶다면 검색조회하면 된다.
