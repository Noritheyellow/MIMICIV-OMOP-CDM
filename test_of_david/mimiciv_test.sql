-- ----------------------------------------------------------------------------
-- 2024.09.25. David Hwang.
--      This code is implemented to confirm how mimiciv(source)'s concepts
--      had mapped with Standard concepts.
--
--      To run this test from the scratch, follow a `README.md` file at root.
--      It's whole brief process is as follow:
--      1) Download and create ATHENA tables(standardized vocabularies).
--      2) Generate custom mapping tables(mimiciv) and combine it 
--         into `tmp_custom_mapping` table which you can find it's 
--         detail procedure in process 7th of `vocabulary_refresh.py`.
--      3) Update custom mapping tables to ATHENA tables. 
--         You can see it's detail in process 8th of `vocabulary_refresh.py`
--         , more precisely `custom_vocabularies.sql`.
--      4) Remove all temporary tables. 
--         Eventually, updated ATHENA tables would only be left.
--         (e.g., concept, concept_relationship, vocabulary, etc.)
-- ----------------------------------------------------------------------------


-- Standard Concepts
SELECT *
  FROM standardized_vocabularies.concept c 
 WHERE standard_concept = 'S'
;


-- To confirm how mimiciv concepts were mapped to Standard concepts.
-- mimiciv 에서만 다루는 개념은 여기에서 찾아보면 될 것 같고,
-- 그 외의 개념은 Standard Concepts를 찾아보면 될 듯.
SELECT cr.*
     , c1.concept_name AS concept_name_1
     , c1.vocabulary_id AS voc_id_1
     , c2.concept_name AS concept_name_2
     , c2.vocabulary_id AS voc_id_1
  FROM standardized_vocabularies.concept_relationship cr
  LEFT JOIN standardized_vocabularies.concept c1 
    ON cr.concept_id_1 = c1.concept_id 
  LEFT JOIN standardized_vocabularies.concept c2 
    ON cr.concept_id_2 = c2.concept_id
 WHERE c1.vocabulary_id ILIKE '%mimic%'
   AND c1.concept_name ILIKE '%F%'
;


-- 이상을 통해서 우리의 json 파일을 구상할 때는 아래와 같은 아이디어로 구상하면 어떨까.

SELECT *
	FROM standardized_vocabularies.concept c
 WHERE concept_id = 8532
;

-- 위와 같은 경우라면 아래와 같다.
-- {
--  "FEMALE": "F"
-- }