-- -------------------------------------------------------------------
-- 2024.09.26. David Hwang.
-- mimiciv.micriobiologyevents의 표준 어휘 사전 
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS mimic_omop_cdm.david_microbiology;
CREATE TABLE mimic_omop_cdm.david_microbiology AS
SELECT ldmc.itemid AS source_concept_code
	 , vc.vocabulary_id      AS source_vocabulary_id 
	 , vc.concept_name       AS source_concept_name
	 , vc.domain_id          AS source_domain_id
	 , vc.concept_class_id   AS source_concept_class_id
	 , vc.concept_id         AS source_concept_id
	 , vcr.relationship_id   AS relationship_id
	 , vcr.concept_id_2      AS target_concept_id
	 , vc2.concept_code      AS target_concept_code
	 , vc2.vocabulary_id     AS target_vocabulary_id
	 , vc2.domain_id         AS target_domain_id
	 , vc2.concept_class_id  AS target_concept_class_id
	FROM mimic_omop_cdm.lk_d_micro_clean ldmc 
	LEFT JOIN mimic_omop_cdm.voc_concept vc 
		ON ldmc.source_code = vc.concept_code 
	 AND ldmc.source_vocabulary_id = vc.vocabulary_id 
	LEFT JOIN mimic_omop_cdm.voc_concept_relationship vcr 
		ON vc.concept_id = vcr.concept_id_1 
	 AND vcr.relationship_id = 'Maps to'
	LEFT JOIN mimic_omop_cdm.voc_concept vc2 
		ON vcr.concept_id_2 =vc2.concept_id 
	 AND vc2.standard_concept = 'S'
	 AND (vc2.invalid_reason IS NULL OR vc2.invalid_reason = '')
 WHERE vc.concept_name IS NOT NULL 
;