-- 2024.09.23. David Hwang.
--      vocabulary_refresh.py 의 7번 과정을 위해 개인적으로 작성한 쿼리 코드이다.
--      본 쿼리는 `custom_mapping_csv/*.csv`를 모두 `tmp_custom_mapping` 테이블로 통합하는 과정을 위한 것이다.
--      기존 코드에서는 BigQuery를 통해서 이 부분을 자동으로 하나로 만들지만, 우리는 PostgreSQL을 사용하므로 이를 위한 코드를 따로 만들고자 한다.
--      
--      vocabulary_refresh/omop_schemas_vocab_bq/custom_mapping.json 를 참고하여 테이블을 생성한다. 그리고 오타가 있다. `reverese -> reverse`

DROP TABLE IF EXISTS custom_mapping.tmp_custom_mapping;
CREATE TABLE custom_mapping.tmp_custom_mapping AS
SELECT *
FROM custom_mapping.gcpt_cs_place_of_service

UNION ALL

SELECT *
FROM custom_mapping.gcpt_drug_ndc

UNION ALL

SELECT *
FROM custom_mapping.gcpt_drug_route

UNION ALL

SELECT *
FROM custom_mapping.gcpt_meas_chartevents_main_mod

UNION ALL

SELECT *
FROM custom_mapping.gcpt_meas_chartevents_value

UNION ALL

SELECT *
FROM custom_mapping.gcpt_meas_lab_loinc_mod

UNION ALL

SELECT *
FROM custom_mapping.gcpt_meas_unit

UNION ALL

SELECT *
FROM custom_mapping.gcpt_meas_waveforms

UNION ALL

SELECT *
FROM custom_mapping.gcpt_micro_antibiotic

UNION ALL

SELECT *
FROM custom_mapping.gcpt_micro_microtest

UNION ALL

SELECT *
FROM custom_mapping.gcpt_micro_organism

UNION ALL

SELECT *
FROM custom_mapping.gcpt_micro_resistance

UNION ALL

SELECT *
FROM custom_mapping.gcpt_micro_specimen

UNION ALL

SELECT *
FROM custom_mapping.gcpt_mimic_generated

UNION ALL

SELECT *
FROM custom_mapping.gcpt_obs_drgcodes

UNION ALL

SELECT *
FROM custom_mapping.gcpt_obs_insurance

UNION ALL

SELECT *
FROM custom_mapping.gcpt_obs_marital

UNION ALL

SELECT *
FROM custom_mapping.gcpt_per_ethnicity

UNION ALL

SELECT *
FROM custom_mapping.gcpt_proc_datetimeevents

UNION ALL

SELECT *
FROM custom_mapping.gcpt_proc_itemid

UNION ALL

SELECT *
FROM custom_mapping.gcpt_vis_admission
;