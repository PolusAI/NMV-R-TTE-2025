

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.67ebd4b5-1028-4053-8924-06ce93a15509"),
    Enriched_Death_Table_Fixed=Input(rid="ri.foundry.main.dataset.00524480-0d9e-4627-98aa-cf9fd061e89f"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41")
)
-- Now we join the death date to the COHORT so that we can filter out patients 

WITH pre1 AS (
SELECT A.person_id
, A.death_date_min
, A.death_date_max
, B.COVID_first_poslab_or_diagnosis_date AS covid_date
, B.data_extraction_date
FROM Enriched_Death_Table_Fixed AS A
INNER JOIN cohort_code AS B 
ON A.person_id = B.person_id

-- -- Below, delete everyone whose min and max death dates are before their COVID diagnosis date ===== WE SHOULD REMOVE THIS FILTER BECAUSE WE STILL WANT TO KNOW IF THEY DIED
----- THESE PATIENTS SHOULD BE DELETED FROM THE ANALYSIS
-- WHERE NOT((death_date_min < COVID_first_poslab_or_diagnosis_date) AND (death_date_max < COVID_first_poslab_or_diagnosis_date))
)

-- In the last step we decide what death date to give the patient
-- If the min and max death dates don't agree, take the first date (min or max date) that occurs AFTER the COVID index date:("person_id","date","data_extraction_date","COVID_patient_death")
SELECT person_id
, CASE 
WHEN (death_date_min <> death_date_max) AND (death_date_min >= covid_date) THEN death_date_min
WHEN (death_date_min <> death_date_max) AND (death_date_max >= covid_date) THEN death_date_max
ELSE death_date_min END AS date
, data_extraction_date
, 1 AS COVID_patient_death
FROM pre1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.00524480-0d9e-4627-98aa-cf9fd061e89f"),
    Enriched_Death_Table=Input(rid="ri.foundry.main.dataset.b07b1c8f-97a7-43b4-826a-299e456c6e85"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41")
)
-- OMOP death table information first (if present), supplemental SSA entries second (if present), followed by PRIVATEOBIT entries, and finally OBITDOTCOM entries.

-- HERE START WITH 1 ROW PP - get their min and max death dates from every unique source
-- The earliest death deats are given priority. 
WITH tab1 AS 
(SELECT 
person_id
, MIN(CASE WHEN datasource_type = 'Original death table' THEN death_date ELSE NULL END) AS OMOP
, MIN(CASE WHEN datasource_type = 'SSA' THEN death_date ELSE NULL END) AS SSA
, MIN(CASE WHEN datasource_type = 'PRIVATEOBIT' THEN death_date ELSE NULL END) AS PRIVATEOBIT
, MIN(CASE WHEN datasource_type = 'OBITDOTCOM' THEN death_date ELSE NULL END) AS OBITDOTCOM

, MAX(CASE WHEN datasource_type = 'Original death table' THEN death_date ELSE NULL END) AS OMOP_max
, MAX(CASE WHEN datasource_type = 'SSA' THEN death_date ELSE NULL END) AS SSA_max
, MAX(CASE WHEN datasource_type = 'PRIVATEOBIT' THEN death_date ELSE NULL END) AS PRIVATEOBIT_max
, MAX(CASE WHEN datasource_type = 'OBITDOTCOM' THEN death_date ELSE NULL END) AS OBITDOTCOM_max
FROM Enriched_Death_Table
GROUP BY 1)

---- CODE BELOW CHECKS IF PEOPLE HAVE DIFFERENT DEATH DATES EVEN AMONG THE SAME SOURCE
-- SELECT COUNT(distinct person_id) FROM tab1
-- WHERE (OMOP <> OMOP_max) OR (SSA <> SSA_max) OR (PRIVATEOBIT <> PRIVATEOBIT_max) OR (OBITDOTCOM <> OBITDOTCOM_max)

--- The code below will choose 1 death date per person; it prioritises the OMOP death date; 
--- It will delete anyone who has a NULL on all date columns 

, tab2 AS (
SELECT person_id
, CASE WHEN OMOP IS NOT NULL THEN OMOP
WHEN SSA IS NOT NULL THEN SSA
WHEN PRIVATEOBIT IS NOT NULL THEN PRIVATEOBIT 
WHEN OBITDOTCOM IS NOT NULL THEN OBITDOTCOM
ELSE NULL
END AS death_date_min

, CASE WHEN OMOP_max IS NOT NULL THEN OMOP_max
WHEN SSA_max IS NOT NULL THEN SSA_max
WHEN PRIVATEOBIT_max IS NOT NULL THEN PRIVATEOBIT_max 
WHEN OBITDOTCOM_max IS NOT NULL THEN OBITDOTCOM_max
ELSE NULL
END AS death_date_max

FROM tab1)

SELECT * FROM tab2 WHERE death_date_min IS NOT NULL

-- SELECT COUNT(*) AS rows, COUNT(distinct person_id) AS N FROM tab1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1a9a8672-d760-4528-902a-ff5c18de18ba"),
    merge_results_HRs=Input(rid="ri.foundry.main.dataset.3ce973a0-3ec4-41f6-acc5-7b71551d38c4"),
    merge_results_rrs=Input(rid="ri.foundry.main.dataset.b5e5c84a-33bd-46d9-8620-5d95eca65f9c")
)
SELECT *, 1 AS Rank_overall, 'Main Analysis' AS Analysis
FROM merge_results_rrs
 
UNION ALL

SELECT *, 2 AS Rank_overall, 'Main Analysis' AS Analysis
FROM merge_results_HRs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4f5729b9-0906-457e-9b81-554e4849d537"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb")
)
-- this counts n treated by DP. "treatment" is time invariant, so this is good to use; 

SELECT data_partner_id
, SUM(treatment) AS number_treated
, SUM(treatment) / COUNT(DISTINCT person_id) AS proportion_treated_within_data_partner
, SUM(treatment) / (SELECT COUNT(DISTINCT person_id) FROM eligible_sample_EHR_all WHERE treatment = 1) AS proportion_treated_among_treated
FROM eligible_sample_EHR_all
GROUP BY 1
ORDER BY 2 DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ff6671cd-bffa-4a1e-93bf-db435109814c"),
    propensity_model_prep_expanded=Input(rid="ri.foundry.main.dataset.9999bcdb-ba34-4487-99b3-64a79b95e279")
)
WITH t1 AS (
    SELECT max(day) OVER(PARTITION BY person_id) AS max_trial, person_id, day
    FROM propensity_model_prep_expanded
)

, T2 AS (
    SELECT * FROM t1 WHERE day = 1
)

-- SELECT approx_percentile(max_trial, 0.5) AS median_trials FROM T2

SELECT * FROM T2

@transform_pandas(
    Output(rid="ri.vector.main.execute.7e52f740-8d3d-46ec-852d-e80be8bd223a"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb")
)
SELECT 
CASE 
WHEN paxlovid_treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Persons' AS denom
, COUNT(DISTINCT person_id) AS group_size
FROM eligible_sample_EHR_all
GROUP BY 1,2

-- UNION ALL

-- SELECT 
-- CASE 
-- WHEN treated = 1 THEN 'PAXLOVID'
-- ELSE 'UNTREATED' END AS treatment
-- , COUNT(DISTINCT person_id) AS group_size
-- FROM eligible_sample_EHR_all
-- GROUP BY 1

-- UNION ALL

-- SELECT 
-- CASE 
-- WHEN treatment = 1 THEN 'PAXLOVID'
-- ELSE 'UNTREATED' END AS treatment
-- , COUNT(DISTINCT person_id) AS group_size
-- FROM eligible_sample_EHR_all
-- GROUP BY 1

UNION ALL

SELECT 
CASE 
WHEN paxlovid_treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Person Trials' AS denom
, COUNT(person_id) AS group_size
FROM eligible_sample_EHR_all
GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.vector.main.execute.6cd9e3ce-dd11-4aad-8fea-bad1ab848083"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Persons' AS denom
, COUNT(DISTINCT person_id) AS group_size
FROM nearest_neighbor_matching_all
GROUP BY 1,2

UNION ALL

SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Person Trials' AS denom
, COUNT(person_id) AS group_size
FROM nearest_neighbor_matching_all
GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.vector.main.execute.c78df2f2-ee2e-4cc4-a2f8-f851e66245b0"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_LC=Input(rid="ri.foundry.main.dataset.6d3eb3b5-af0e-4831-9d10-c593c7158038"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Persons' AS denom
, COUNT(DISTINCT person_id) AS group_size
FROM nearest_neighbor_matching_LC
GROUP BY 1,2

UNION ALL

SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Person Trials' AS denom
, COUNT(person_id) AS group_size
FROM nearest_neighbor_matching_LC
GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.vector.main.execute.9f1804c7-4b71-4ee5-8377-76f8dd802685"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
, trial
,'Persons' AS denom
, COUNT(DISTINCT subclass) AS group_size
FROM nearest_neighbor_matching_all
GROUP BY 1,2,3
ORDER BY denom, treatment, trial

-- UNION ALL

-- SELECT 
-- CASE 
-- WHEN treatment = 1 THEN 'PAXLOVID'
-- ELSE 'UNTREATED' END AS treatment
-- ,'Person Trials' AS denom
-- , COUNT(person_id) AS group_size
-- FROM nearest_neighbor_matching_all
-- GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.vector.main.execute.1987aa01-2f8f-4f9c-83f6-c2e834bca614"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    nearest_neighbor_matching_LC=Input(rid="ri.foundry.main.dataset.6d3eb3b5-af0e-4831-9d10-c593c7158038"),
    nearest_neighbor_matching_all=Input(rid="ri.foundry.main.dataset.7ed3e283-f6c6-43f0-baf6-ee29ea81c982")
)
SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
, trial
,'Persons' AS denom
, COUNT(DISTINCT subclass) AS group_size
FROM nearest_neighbor_matching_LC
GROUP BY 1,2,3
ORDER BY denom, treatment, trial

-- UNION ALL

-- SELECT 
-- CASE 
-- WHEN treatment = 1 THEN 'PAXLOVID'
-- ELSE 'UNTREATED' END AS treatment
-- ,'Person Trials' AS denom
-- , COUNT(person_id) AS group_size
-- FROM nearest_neighbor_matching_all
-- GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.vector.main.execute.96f3ead0-4326-4860-9173-bfd2bbd8aa40"),
    eligible_sample_EHR_all=Input(rid="ri.foundry.main.dataset.bb3f1155-74ee-4013-b621-67e08eca5aeb"),
    propensity_model_all_weights_formatching_LC=Input(rid="ri.foundry.main.dataset.41cf61a7-c055-49df-aa97-408c434c6a9f")
)
SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Persons' AS denom
, COUNT(DISTINCT person_id) AS group_size
FROM propensity_model_all_weights_formatching_LC
GROUP BY 1,2

UNION ALL

SELECT 
CASE 
WHEN treatment = 1 THEN 'PAXLOVID'
ELSE 'UNTREATED' END AS treatment
,'Person Trials' AS denom
, COUNT(person_id) AS group_size
FROM propensity_model_all_weights_formatching_LC
GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.90b57f36-aaeb-4f6d-8831-6141389595e0"),
    filtered_lab_measures_harmonized_cleaned_pt2=Input(rid="ri.foundry.main.dataset.af1b547a-9484-470b-ba37-fd3f11d31ae2")
)
-- THE PURPOSE OF THIS NODE IS TO TAKE A SINGLE LAB MEASURE FOR A SINGLE DAY - WE MUST DECIDE TO TAKE THE MEAN, MAX, OR MIN
-- WE HAVE A PREFERENCE TO SELECT THE VALUE THAT IS ABNORMAL, IF IT IS PRESENT ON A GIVEN DAY FOR A PATIENT

SELECT 
person_id
, date

, CASE 
WHEN CRP_MAX = CRP_MIN THEN CRP
WHEN (CRP_MIN BETWEEN 0 AND 10) AND (CRP_MAX BETWEEN 5 AND 10) THEN CRP -- if both in normal range, use the average 

-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (CRP_MIN BETWEEN 0 AND 10) THEN CRP_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (CRP_MAX BETWEEN 0 AND 10) THEN CRP_MIN -- if the maximum is normal, take the minimum value which is outside the normal range

-- Below is when the BAD value only falls in one direction; BUT both are abnormal. Select only one
WHEN GREATEST(CRP_MIN, CRP_MIN) > 10 THEN GREATEST(CRP_MIN, CRP_MIN) 
ELSE CRP_MAX END AS CRP

, CASE 
WHEN WBC_MAX = WBC_MIN THEN WBC
WHEN (WBC_MIN BETWEEN 4500 AND 11000) AND (WBC_MAX BETWEEN 4500 AND 11000) THEN WBC -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (WBC_MIN BETWEEN 4500 AND 11000) THEN WBC_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (WBC_MAX BETWEEN 4500 AND 11000) THEN WBC_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (4500 - WBC_MIN) > (WBC_MAX - 11000) THEN WBC_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (WBC_MAX - 11000) >= (4500 - WBC_MIN) THEN WBC_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value

ELSE WBC END AS WBC

, CASE 
WHEN LYMPH_MAX = LYMPH_MIN THEN LYMPH
WHEN (LYMPH_MIN BETWEEN 1000 AND 4800) AND (LYMPH_MAX BETWEEN 1000 AND 4800) THEN LYMPH -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (LYMPH_MIN BETWEEN 1000 AND 4800) THEN LYMPH_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (LYMPH_MAX BETWEEN 1000 AND 4800) THEN LYMPH_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (1000 - LYMPH_MIN) > (LYMPH_MAX - 4800) THEN LYMPH_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (LYMPH_MAX - 4800) >= (1000 - LYMPH_MIN) THEN LYMPH_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
ELSE LYMPH END AS LYMPH

, CASE 
WHEN ALBUMIN_MAX = ALBUMIN_MIN THEN ALBUMIN
WHEN (ALBUMIN_MIN BETWEEN 3.4 AND 5.4) AND (ALBUMIN_MAX BETWEEN 3.4 AND 5.4) THEN ALBUMIN -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (ALBUMIN_MIN BETWEEN 3.4 AND 5.4) THEN ALBUMIN_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (ALBUMIN_MAX BETWEEN 3.4 AND 5.4) THEN ALBUMIN_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (3.4 - ALBUMIN_MIN) > (ALBUMIN_MAX - 5.4) THEN ALBUMIN_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (ALBUMIN_MAX - 5.4) >= (3.4 - ALBUMIN_MIN) THEN ALBUMIN_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
ELSE ALBUMIN END AS ALBUMIN

, CASE 
WHEN ALT_MAX = ALT_MIN THEN ALT
WHEN (ALT_MIN BETWEEN 7 AND 56) AND (ALT_MAX BETWEEN 7 AND 56) THEN ALT -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (ALT_MIN BETWEEN 7 AND 56) THEN ALT_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (ALT_MAX BETWEEN 7 AND 56) THEN ALT_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (7 - ALT_MIN) > (ALT_MAX - 56) THEN ALT_MIN -- if the discrepancy is LARGER in the lower direction (i.e., the lower limit of the normal range MINUS the person's minimum value), take that lower value
WHEN (ALT_MAX - 56) >= (7 - ALT_MIN) THEN ALT_MAX -- if the discrepancy is LARGER in the opposite direction or equal (i.e., the person's max value MINUS the upper limit of the normal range), take the upper value
ELSE ALT END AS ALT

-- CHANGED EGFR to 90
, CASE 
WHEN EGFR_MAX = EGFR_MIN THEN EGFR
WHEN (EGFR_MIN >= 90) AND (EGFR_MAX >= 90) THEN EGFR -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (EGFR_MIN >= 90) THEN EGFR_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (EGFR_MAX >= 90) THEN EGFR_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD value only falls in one direction
WHEN LEAST(EGFR_MIN, EGFR_MIN) < 90 THEN LEAST(EGFR_MIN, EGFR_MIN) 
ELSE EGFR END AS EGFR

, CASE 
WHEN DIABETES_A1C_MAX = DIABETES_A1C_MIN THEN DIABETES_A1C
WHEN (DIABETES_A1C_MIN <= 6.5) AND (DIABETES_A1C_MAX <= 6.5) THEN DIABETES_A1C -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (DIABETES_A1C_MIN <= 6.5) THEN DIABETES_A1C_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (DIABETES_A1C_MAX <= 6.5) THEN DIABETES_A1C_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD value only falls in one direction
WHEN LEAST(DIABETES_A1C_MIN, DIABETES_A1C_MIN) > 6.5 THEN LEAST(DIABETES_A1C_MIN, DIABETES_A1C_MIN) 
ELSE DIABETES_A1C END AS DIABETES_A1C

-- , CASE 
-- WHEN DDIMER_MAX = DDIMER_MIN THEN DDIMER
-- WHEN (DDIMER_MIN >= 0.5) AND (DDIMER_MAX >= 0.5) THEN DDIMER -- if both in normal range, use the average 
-- -- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
-- WHEN (DDIMER_MIN >= 0.5) THEN DDIMER_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (DDIMER_MAX >= 0.5) THEN DDIMER_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- -- Below is when the BAD value only falls in one direction
-- WHEN LEAST(DDIMER_MIN, DDIMER_MIN) < 0.5 THEN LEAST(DDIMER_MIN, DDIMER_MIN) 
-- ELSE DDIMER END AS DDIMER

, CASE 
-- FIRST FOR MALES
WHEN HEMOGLOBIN_MAX = HEMOGLOBIN_MIN THEN HEMOGLOBIN
WHEN (HEMOGLOBIN_MIN BETWEEN 13 AND 17) AND (HEMOGLOBIN_MAX BETWEEN 13 AND 17) AND sex = 'MALE' THEN HEMOGLOBIN -- if both in normal range, use the average 
WHEN (HEMOGLOBIN_MIN BETWEEN 12 AND 16) AND (HEMOGLOBIN_MAX BETWEEN 12 AND 16) AND sex = 'FEMALE' THEN HEMOGLOBIN -- if both in normal range, use the average
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (HEMOGLOBIN_MIN BETWEEN 13 AND 17) AND sex = 'MALE' THEN HEMOGLOBIN_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (HEMOGLOBIN_MIN BETWEEN 12 AND 16) AND sex = 'FEMALE' THEN HEMOGLOBIN_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (HEMOGLOBIN_MAX BETWEEN 13 AND 17) AND sex = 'MALE' THEN HEMOGLOBIN_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
WHEN (HEMOGLOBIN_MAX BETWEEN 12 AND 16) AND sex = 'FEMALE' THEN HEMOGLOBIN_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (13 - HEMOGLOBIN_MIN) > (HEMOGLOBIN_MAX - 17) AND sex = 'MALE' THEN HEMOGLOBIN_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (12 - HEMOGLOBIN_MIN) > (HEMOGLOBIN_MAX - 16) AND sex = 'FEMALE' THEN HEMOGLOBIN_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (HEMOGLOBIN_MAX - 17) >= (13 - HEMOGLOBIN_MIN) AND sex = 'MALE' THEN HEMOGLOBIN_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
WHEN (HEMOGLOBIN_MAX - 16) >= (12 - HEMOGLOBIN_MIN) AND sex = 'FEMALE' THEN HEMOGLOBIN_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
ELSE HEMOGLOBIN END AS HEMOGLOBIN

, CASE 
WHEN CREATININE_MAX = CREATININE_MIN THEN CREATININE
WHEN (CREATININE_MIN BETWEEN 0.5 AND 7.5) AND (CREATININE_MAX BETWEEN 0.5 AND 7.5) THEN CREATININE -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (CREATININE_MIN BETWEEN 0.5 AND 7.5) THEN CREATININE_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (CREATININE_MAX BETWEEN 0.5 AND 7.5) THEN CREATININE_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- For creatinine if both are abnormal, we take the HIGH value. 
WHEN GREATEST(CREATININE_MIN, CREATININE_MAX) > 7.5 THEN GREATEST(CREATININE_MIN, CREATININE_MAX)
ELSE CREATININE END AS CREATININE

, CASE 
WHEN PLT_MAX = PLT_MIN THEN PLT
WHEN (PLT_MIN BETWEEN 140000 AND 450000) AND (PLT_MAX BETWEEN 140000 AND 450000) THEN PLT -- if both in normal range, use the average 
-- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
WHEN (PLT_MIN BETWEEN 140000 AND 450000) THEN PLT_MAX -- if the minimum is normal, take the maximum which is outside the normal range
WHEN (PLT_MAX BETWEEN 140000 AND 450000) THEN PLT_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- Below is when the BAD falls in both directions
WHEN (140000 - PLT_MIN) > (PLT_MAX - 450000) THEN PLT_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
WHEN (PLT_MAX - 450000) >= (140000 - PLT_MIN) THEN PLT_MAX -- if the discrepancy is LARGER in the higher direction or equal, take the upper value
ELSE PLT END AS PLT

-- , DBP
-- , SBP

-- ----- ADDITIONAL  MEASURES
-- -- NEUTROPHILS - range is 2500 and 7000
-- , CASE 
-- WHEN NEUTRO_MAX = NEUTRO_MIN THEN NEUTRO
-- WHEN (NEUTRO_MIN BETWEEN 2500 AND 7000) AND (NEUTRO_MAX BETWEEN 2500 AND 7000) THEN NEUTRO -- if both in normal range, use the average 
-- -- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
-- WHEN (NEUTRO_MIN BETWEEN 2500 AND 7000) THEN NEUTRO_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (NEUTRO_MAX BETWEEN 2500 AND 7000) THEN NEUTRO_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- -- Below is when the BAD falls in both directions
-- WHEN (2500 - NEUTRO_MIN) > (NEUTRO_MAX - 7000) THEN NEUTRO_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
-- WHEN (NEUTRO_MAX - 7000) >= (2500 - NEUTRO_MIN) THEN NEUTRO_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
-- ELSE NEUTRO END AS NEUTRO

-- , CASE 
-- -- FIRST FOR MALES
-- WHEN FERRITIN_MAX = FERRITIN_MIN THEN FERRITIN
-- WHEN (FERRITIN_MIN BETWEEN 30 AND 300) AND (FERRITIN_MAX BETWEEN 30 AND 300) AND sex = 'MALE' THEN FERRITIN -- if both in normal range, use the average 
-- WHEN (FERRITIN_MIN BETWEEN 10 AND 200) AND (FERRITIN_MAX BETWEEN 10 AND 200) AND sex = 'FEMALE' THEN FERRITIN -- if both in normal range, use the average
-- -- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
-- WHEN (FERRITIN_MIN BETWEEN 30 AND 300) AND sex = 'MALE' THEN FERRITIN_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (FERRITIN_MIN BETWEEN 10 AND 200) AND sex = 'FEMALE' THEN FERRITIN_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (FERRITIN_MAX BETWEEN 30 AND 300) AND sex = 'MALE' THEN FERRITIN_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- WHEN (FERRITIN_MAX BETWEEN 10 AND 200) AND sex = 'FEMALE' THEN FERRITIN_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- -- Below is when the BAD falls in both directions
-- WHEN (30 - FERRITIN_MIN) > (FERRITIN_MAX - 300) AND sex = 'MALE' THEN FERRITIN_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
-- WHEN (10 - FERRITIN_MIN) > (FERRITIN_MAX - 200) AND sex = 'FEMALE' THEN FERRITIN_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
-- WHEN (FERRITIN_MAX - 300) >= (30 - FERRITIN_MIN) AND sex = 'MALE' THEN FERRITIN_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
-- WHEN (FERRITIN_MAX - 200) >= (10 - FERRITIN_MIN) AND sex = 'FEMALE' THEN FERRITIN_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
-- ELSE FERRITIN END AS FERRITIN

-- , CASE 
-- WHEN LDH_MAX = LDH_MIN THEN LDH
-- WHEN (LDH_MIN BETWEEN 140 AND 280) AND (LDH_MAX BETWEEN 140 AND 280) THEN LDH -- if both in normal range, use the average 
-- -- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
-- WHEN (LDH_MIN BETWEEN 140 AND 280) THEN LDH_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (LDH_MAX BETWEEN 140 AND 280) THEN LDH_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- -- Below is when the BAD falls in both directions
-- WHEN (140 - LDH_MIN) > (LDH_MAX - 280) THEN LDH_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
-- WHEN (LDH_MAX - 280) >= (140 - LDH_MIN) THEN LDH_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
-- ELSE LDH END AS LDH

-- , CASE 
-- WHEN UREA_MAX = UREA_MIN THEN UREA
-- WHEN (UREA_MIN BETWEEN 6 AND 23) AND (UREA_MAX BETWEEN 6 AND 23) THEN UREA -- if both in normal range, use the average 
-- -- Below is if either ONE of the max or min is abnormal. We take the abnormal one. 
-- WHEN (UREA_MIN BETWEEN 6 AND 23) THEN UREA_MAX -- if the minimum is normal, take the maximum which is outside the normal range
-- WHEN (UREA_MAX BETWEEN 6 AND 23) THEN UREA_MIN -- if the maximum is normal, take the minimum value which is outside the normal range
-- -- Below is when the BAD falls in both directions
-- WHEN (6 - UREA_MIN) > (UREA_MAX - 23) THEN UREA_MIN -- if the discrepancy is LARGER in the lower direction, take that lower value
-- WHEN (UREA_MAX - 23) >= (6 - UREA_MIN) THEN UREA_MAX -- if the discrepancy is LARGER in the opposite direction or equal, take the upper value
-- ELSE UREA END AS UREA

FROM filtered_lab_measures_harmonized_cleaned_pt2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d6bb3103-c554-4657-9764-8a8a98946f92"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    customize_concept_sets=Input(rid="ri.foundry.main.dataset.de3b19a8-bb5d-4752-a040-4e338b103af9"),
    deaths_filtered=Input(rid="ri.foundry.main.dataset.415dd063-bba0-4954-8f26-e9a4ff5dc77f"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
-- This version uses the harmonized value except for EGFR which doesnt exist.
-- This node also removes invalid concepts from the concept set of each lab measure - i.e., concepts that are in INCONSISTENT units to the other concepts in the set. 

WITH lab_concepts_df AS 
(
SELECT 
A.concept_set_name
, B.indicator_prefix
, A.concept_id
FROM concept_set_members AS A
INNER JOIN customize_concept_sets AS B
ON A.concept_set_name = B.concept_set_name
WHERE A.is_most_recent_version = TRUE
)

, measurement_df AS 
(
SELECT 
A.person_id
, A.measurement_date
, A.measurement_concept_id
, A.measurement_concept_name
, A.harmonized_value_as_number
, A.value_as_number
, A.value_as_concept_id
, B.concept_set_name
, B.indicator_prefix

FROM measurement A
INNER JOIN lab_concepts_df AS B
ON A.measurement_concept_id = B.concept_id
)

SELECT 
A.person_id
, measurement_date
, measurement_concept_id
, measurement_concept_name
, CASE WHEN indicator_prefix = 'EGFR' THEN value_as_number ELSE harmonized_value_as_number END AS harmonized_value_as_number
, concept_set_name
, indicator_prefix
, B.sex
FROM measurement_df AS A
INNER JOIN cohort_code AS B
ON A.person_id = B.person_id

WHERE measurement_concept_name NOT IN ('C reactive protein [Quintile] in Serum or Plasma by High sensitivity method','Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Cystatin C-based formula','Lymphocyte count','Lymphocytes [#/volume] in Specimen by Automated count','Platelets [#/volume] in Blood by Estimate','White blood cell count','Blood urea nitrogen measurement','Urea nitrogen [Mass ratio] in Serum or Plasma--post dialysis/pre dialysis','Serum ferritin measurement')

-- -- SELECT person_id, measurement_date AS date
-- -- , MAX(CASE WHEN indicator_prefix = 'CRP' THEN harmonized_value_as_number ELSE NULL END) AS CRP
-- -- , MAX(CASE WHEN indicator_prefix = 'WBC' THEN harmonized_value_as_number ELSE NULL END) AS WBC
-- -- , MAX(CASE WHEN indicator_prefix = 'LYMPH' THEN harmonized_value_as_number ELSE NULL END) AS LYMPH
-- -- , MAX(CASE WHEN indicator_prefix = 'ALBUMIN' THEN harmonized_value_as_number ELSE NULL END) AS ALBUMIN
-- -- , MAX(CASE WHEN indicator_prefix = 'ALT' THEN harmonized_value_as_number ELSE NULL END) AS ALT
-- -- , MAX(CASE WHEN indicator_prefix = 'EGFR' THEN harmonized_value_as_number ELSE NULL END) AS EGFR
-- -- , MAX(CASE WHEN indicator_prefix = 'DDIMER' THEN harmonized_value_as_number ELSE NULL END) AS DDIMER
-- -- , MAX(CASE WHEN indicator_prefix = 'CREATININE' THEN harmonized_value_as_number ELSE NULL END) AS CREATININE
-- -- , MAX(CASE WHEN indicator_prefix = 'PLT' THEN harmonized_value_as_number ELSE NULL END) AS PLT
-- -- , MAX(CASE WHEN indicator_prefix = 'SBP' THEN harmonized_value_as_number ELSE NULL END) AS SBP
-- -- , MAX(CASE WHEN indicator_prefix = 'DBP' THEN harmonized_value_as_number ELSE NULL END) AS DBP
-- -- FROM long_table
-- -- GROUP BY 1,2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.af1b547a-9484-470b-ba37-fd3f11d31ae2"),
    filtered_lab_measures_harmonized_cleaned_pt1=Input(rid="ri.foundry.main.dataset.d6bb3103-c554-4657-9764-8a8a98946f92")
)
WITH long_table AS

(
SELECT * FROM filtered_lab_measures_harmonized_cleaned_pt1
WHERE measurement_date IS NOT NULL
)

-- here we fix up zero values on some columns; make them NULLS. Removed EGFR from this list, because 0 is possible. But only if on RRP. 

, remove_zeros AS 
(
SELECT person_id, measurement_date, sex, indicator_prefix
, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY measurement_date) AS measure_count
, CASE WHEN indicator_prefix IN ('ALBUMIN','ALT','CREATININE','CRP','HEMOGLOBIN','LYMPH','PLT','WBC','DIABETES_A1C') AND harmonized_value_as_number = 0
THEN NULL ELSE harmonized_value_as_number END AS harmonized_value_as_number
FROM long_table

)

-- HERE WE HARMONIZE UNITS ON SOME OF THE LAB MEASUREMENT COLUMNS; 

, harmonized_units AS 
(
SELECT person_id, measurement_date, sex, indicator_prefix
, MAX(measure_count) OVER(PARTITION BY person_id, measurement_date) AS measure_count -- This will indicate that the person had multiple measures in a single day
, CASE 
WHEN indicator_prefix = 'CRP' THEN harmonized_value_as_number/10
WHEN indicator_prefix = 'WBC' AND harmonized_value_as_number <= 1000 THEN harmonized_value_as_number*1000
WHEN indicator_prefix = 'LYMPH' AND harmonized_value_as_number <= 1000 THEN harmonized_value_as_number*1000

-- -- NEW MEASURES --------------------
-- WHEN indicator_prefix = 'NEUTRO' AND harmonized_value_as_number <= 1000 THEN harmonized_value_as_number*1000
-- ------------------------------------

WHEN indicator_prefix = 'ALT' AND harmonized_value_as_number > 1000 THEN harmonized_value_as_number/1000
WHEN indicator_prefix = 'EGFR' AND harmonized_value_as_number > 1000 THEN harmonized_value_as_number/1000
-- WHEN indicator_prefix = 'DDIMER' AND harmonized_value_as_number > 100 THEN harmonized_value_as_number/100
WHEN indicator_prefix = 'PLT' THEN harmonized_value_as_number * 1000
ELSE harmonized_value_as_number END AS harmonized_value_as_number
FROM remove_zeros
)

-- THIS STEP MAKES VALUES HIGHER THAN THE REPORTABLE RANGE OF EACH MEASUREMENT NULL (WINSORIZING)

, filtered_values AS 
(
SELECT person_id, measurement_date, sex, indicator_prefix
, MAX(measure_count) OVER(PARTITION BY person_id) AS measure_count -- This will indicate that the person had multiple measures in a single day
, CASE 
WHEN indicator_prefix = 'CRP' AND harmonized_value_as_number > 160 THEN NULL 
WHEN indicator_prefix = 'WBC' AND harmonized_value_as_number > 99900 THEN NULL
WHEN indicator_prefix = 'LYMPH' AND harmonized_value_as_number > 50949 THEN NULL
WHEN indicator_prefix = 'ALBUMIN' AND harmonized_value_as_number > 7 THEN NULL
WHEN indicator_prefix = 'ALT' AND harmonized_value_as_number > 400 THEN NULL
WHEN indicator_prefix = 'EGFR' AND harmonized_value_as_number > 202.7 THEN NULL
-- WHEN indicator_prefix = 'DDIMER' AND harmonized_value_as_number > 18.8 THEN NULL
WHEN indicator_prefix = 'PLT' AND harmonized_value_as_number > 999000 THEN NULL
WHEN indicator_prefix = 'HEMOGLOBIN' AND harmonized_value_as_number > 25 THEN NULL
WHEN indicator_prefix = 'CREATININE' AND harmonized_value_as_number > 25 THEN NULL
WHEN indicator_prefix = 'DIABETES_A1C' AND harmonized_value_as_number > 100 THEN NULL

-- -- additional measures
-- WHEN indicator_prefix = 'NEUTRO' AND harmonized_value_as_number > 21000 THEN NULL -- https://pmc.ncbi.nlm.nih.gov/articles/PMC7491404/ (mean = 7.85, SD = 3.18) 
-- WHEN indicator_prefix = 'UREA' AND harmonized_value_as_number > 150 THEN NULL -- based on NHANES report
-- WHEN indicator_prefix = 'LDH' AND harmonized_value_as_number > 1000 THEN NULL -- updated based on NHANES report
-- WHEN indicator_prefix = 'FERRITIN' AND harmonized_value_as_number > 2500 THEN NULL -- normal range (10 - 800) -- updated based on NHANES report
-- ----------------------------

ELSE harmonized_value_as_number END AS harmonized_value_as_number
FROM remove_zeros
)

--- NOW - for each lab measure take the max, min, and mean per PERSON-DAY

SELECT person_id, measurement_date AS date, sex
, MAX(measure_count) AS measure_count
, AVG(CASE WHEN indicator_prefix = 'CRP' THEN harmonized_value_as_number ELSE NULL END) AS CRP
, AVG(CASE WHEN indicator_prefix = 'WBC' THEN harmonized_value_as_number ELSE NULL END) AS WBC
, AVG(CASE WHEN indicator_prefix = 'LYMPH' THEN harmonized_value_as_number ELSE NULL END) AS LYMPH
, AVG(CASE WHEN indicator_prefix = 'ALBUMIN' THEN harmonized_value_as_number ELSE NULL END) AS ALBUMIN
, AVG(CASE WHEN indicator_prefix = 'ALT' THEN harmonized_value_as_number ELSE NULL END) AS ALT
, AVG(CASE WHEN indicator_prefix = 'EGFR' THEN harmonized_value_as_number ELSE NULL END) AS EGFR
-- , AVG(CASE WHEN indicator_prefix = 'DDIMER' THEN harmonized_value_as_number ELSE NULL END) AS DDIMER
, AVG(CASE WHEN indicator_prefix = 'CREATININE' THEN harmonized_value_as_number ELSE NULL END) AS CREATININE
, AVG(CASE WHEN indicator_prefix = 'PLT' THEN harmonized_value_as_number ELSE NULL END) AS PLT
-- , AVG(CASE WHEN indicator_prefix = 'SBP' THEN harmonized_value_as_number ELSE NULL END) AS SBP
-- , AVG(CASE WHEN indicator_prefix = 'DBP' THEN harmonized_value_as_number ELSE NULL END) AS DBP
, AVG(CASE WHEN indicator_prefix = 'HEMOGLOBIN' THEN harmonized_value_as_number ELSE NULL END) AS HEMOGLOBIN
, AVG(CASE WHEN indicator_prefix = 'DIABETES_A1C' THEN harmonized_value_as_number ELSE NULL END) AS DIABETES_A1C
-- ---- new measures
-- , AVG(CASE WHEN indicator_prefix = 'NEUTRO' THEN harmonized_value_as_number ELSE NULL END) AS NEUTRO
-- , AVG(CASE WHEN indicator_prefix = 'UREA' THEN harmonized_value_as_number ELSE NULL END) AS UREA
-- , AVG(CASE WHEN indicator_prefix = 'LDH' THEN harmonized_value_as_number ELSE NULL END) AS LDH
-- , AVG(CASE WHEN indicator_prefix = 'FERRITIN' THEN harmonized_value_as_number ELSE NULL END) AS FERRITIN

-- MAXIMUMS

, MAX(CASE WHEN indicator_prefix = 'CRP' THEN harmonized_value_as_number ELSE NULL END) AS CRP_MAX
, MAX(CASE WHEN indicator_prefix = 'WBC' THEN harmonized_value_as_number ELSE NULL END) AS WBC_MAX
, MAX(CASE WHEN indicator_prefix = 'LYMPH' THEN harmonized_value_as_number ELSE NULL END) AS LYMPH_MAX
, MAX(CASE WHEN indicator_prefix = 'ALBUMIN' THEN harmonized_value_as_number ELSE NULL END) AS ALBUMIN_MAX
, MAX(CASE WHEN indicator_prefix = 'ALT' THEN harmonized_value_as_number ELSE NULL END) AS ALT_MAX
, MAX(CASE WHEN indicator_prefix = 'EGFR' THEN harmonized_value_as_number ELSE NULL END) AS EGFR_MAX
-- , MAX(CASE WHEN indicator_prefix = 'DDIMER' THEN harmonized_value_as_number ELSE NULL END) AS DDIMER_MAX
, MAX(CASE WHEN indicator_prefix = 'CREATININE' THEN harmonized_value_as_number ELSE NULL END) AS CREATININE_MAX
, MAX(CASE WHEN indicator_prefix = 'PLT' THEN harmonized_value_as_number ELSE NULL END) AS PLT_MAX
-- , MAX(CASE WHEN indicator_prefix = 'SBP' THEN harmonized_value_as_number ELSE NULL END) AS SBP_MAX
-- , MAX(CASE WHEN indicator_prefix = 'DBP' THEN harmonized_value_as_number ELSE NULL END) AS DBP_MAX
, MAX(CASE WHEN indicator_prefix = 'HEMOGLOBIN' THEN harmonized_value_as_number ELSE NULL END) AS HEMOGLOBIN_MAX
, MAX(CASE WHEN indicator_prefix = 'DIABETES_A1C' THEN harmonized_value_as_number ELSE NULL END) AS DIABETES_A1C_MAX

-- -- newmeasures
-- , MAX(CASE WHEN indicator_prefix = 'NEUTRO' THEN harmonized_value_as_number ELSE NULL END) AS NEUTRO_MAX
-- , MAX(CASE WHEN indicator_prefix = 'UREA' THEN harmonized_value_as_number ELSE NULL END) AS UREA_MAX
-- , MAX(CASE WHEN indicator_prefix = 'LDH' THEN harmonized_value_as_number ELSE NULL END) AS LDH_MAX
-- , MAX(CASE WHEN indicator_prefix = 'FERRITIN' THEN harmonized_value_as_number ELSE NULL END) AS FERRITIN_MAX

-- MINIMUMS

, MIN(CASE WHEN indicator_prefix = 'CRP' THEN harmonized_value_as_number ELSE NULL END) AS CRP_MIN
, MIN(CASE WHEN indicator_prefix = 'WBC' THEN harmonized_value_as_number ELSE NULL END) AS WBC_MIN
, MIN(CASE WHEN indicator_prefix = 'LYMPH' THEN harmonized_value_as_number ELSE NULL END) AS LYMPH_MIN
, MIN(CASE WHEN indicator_prefix = 'ALBUMIN' THEN harmonized_value_as_number ELSE NULL END) AS ALBUMIN_MIN
, MIN(CASE WHEN indicator_prefix = 'ALT' THEN harmonized_value_as_number ELSE NULL END) AS ALT_MIN
, MIN(CASE WHEN indicator_prefix = 'EGFR' THEN harmonized_value_as_number ELSE NULL END) AS EGFR_MIN
, MIN(CASE WHEN indicator_prefix = 'DDIMER' THEN harmonized_value_as_number ELSE NULL END) AS DDIMER_MIN
, MIN(CASE WHEN indicator_prefix = 'CREATININE' THEN harmonized_value_as_number ELSE NULL END) AS CREATININE_MIN
, MIN(CASE WHEN indicator_prefix = 'PLT' THEN harmonized_value_as_number ELSE NULL END) AS PLT_MIN
-- , MIN(CASE WHEN indicator_prefix = 'SBP' THEN harmonized_value_as_number ELSE NULL END) AS SBP_MIN
-- , MIN(CASE WHEN indicator_prefix = 'DBP' THEN harmonized_value_as_number ELSE NULL END) AS DBP_MIN
, MIN(CASE WHEN indicator_prefix = 'HEMOGLOBIN' THEN harmonized_value_as_number ELSE NULL END) AS HEMOGLOBIN_MIN
, MIN(CASE WHEN indicator_prefix = 'DIABETES_A1C' THEN harmonized_value_as_number ELSE NULL END) AS DIABETES_A1C_MIN
-- -- new measures
-- , MIN(CASE WHEN indicator_prefix = 'NEUTRO' THEN harmonized_value_as_number ELSE NULL END) AS NEUTRO_MIN
-- , MIN(CASE WHEN indicator_prefix = 'UREA' THEN harmonized_value_as_number ELSE NULL END) AS UREA_MIN
-- , MIN(CASE WHEN indicator_prefix = 'LDH' THEN harmonized_value_as_number ELSE NULL END) AS LDH_MIN
-- , MIN(CASE WHEN indicator_prefix = 'FERRITIN' THEN harmonized_value_as_number ELSE NULL END) AS FERRITIN_MIN

FROM harmonized_units
GROUP BY 1,2, 3

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.20a8907e-c506-4116-a04c-ad96436639f8"),
    cohort_code=Input(rid="ri.foundry.main.dataset.b6450c6b-55d3-41e3-8208-14fa0780cd41"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    minimal_patient_cohort=Input(rid="ri.foundry.main.dataset.9b0b693d-1e29-4eeb-aa02-a86f1c7c7fb6")
)
-- This node creates a cross join of each patient and 31 days starting from their COVID index date. We only need 7 days for the metformin study
-- Each DAY is baseline for a sequential trial; 

-- 1. select the pool of (minimally) eligible patients; 
WITH persons AS 
(SELECT DISTINCT person_id 
FROM minimal_patient_cohort
)

-- 2. Create a table of distinct dates from: December 30, 2020 (this is what was used in the COVID-out trial)
, dates AS 
(SELECT distinct visit_start_date AS date 
FROM microvisits_to_macrovisits 
WHERE visit_start_date BETWEEN '2021-12-01' AND CURRENT_DATE)

-- Now create a complete cross join 
, person_date_cross_join AS (
SELECT person_id, date
FROM persons
CROSS JOIN dates)
 
-- 3. Filter the above table to 7 days for each pateint, starting from their COVID index day
-- 4. Create a one row PP table, with their COVID index day PLUS the last day to enter a trial (day 7)
, patients_start_end_dates AS
(SELECT DISTINCT A.person_id
, A.COVID_first_poslab_or_diagnosis_date AS start_day
, DATE_ADD(A.COVID_first_poslab_or_diagnosis_date, 5) AS final_day
FROM minimal_patient_cohort AS A 
INNER JOIN persons AS B
ON A.person_id = B.person_id)

-- Merge Cross Join table to the Table above; Filter the dates in the cross join table to each patient's start and end day. 
, merged AS 
(SELECT A.person_id
, A.start_day
, A.final_day
, B.date
FROM patients_start_end_dates AS A
INNER JOIN person_date_cross_join AS B
ON A.person_id = B.person_id
WHERE B.date BETWEEN A.start_day AND A.final_day
)

-- We want to output the dataset to include a rownumber variable which is TIME (from baseline - 1st day of hospitalization)
-- Furthermore, we are going to limit the number of rows to the first 100 days only (this is 100 days from the start of COVID hospitalization, AND may include the dates after)
, prefinal1 AS 
(
SELECT 
person_id
, start_day -- I HAVE ADDED THIS AUGUST 16th
, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY date) AS time
, date
FROM merged 
)

SELECT * FROM prefinal1 

@transform_pandas(
    Output(rid="ri.vector.main.execute.dffe53e2-ae4a-43c9-a4ab-b9f07ee8bad6"),
    long_dataset_all=Input(rid="ri.foundry.main.dataset.fd0ccf46-d321-41d5-a119-2e57940a931d")
)
WITH t1 AS (SELECT person_id, day, treated, CASE WHEN MAX(treated) OVER(PARTITION BY person_id) > 0 THEN 1 ELSE 0 END AS treated_ever
FROM long_dataset_all

)

SELECT * FROM t1
-- WHERE day = 6 and treated = 1
-- WHERE treated_ever = 0
WHERE person_id = '1005991731621945870'

@transform_pandas(
    Output(rid="ri.vector.main.execute.abbc6834-8500-420f-afba-52f788aabad1"),
    baseline_variables_stacked=Input(rid="ri.foundry.main.dataset.e910efc8-0261-40da-86f1-2f90e2d5e44b")
)
WITH t1 AS (

    SELECT person_id, date, ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY date) AS day, paxlovid_treatment, MAX(paxlovid_treatment) OVER(PARTITION BY person_id) AS treated_ever
    FROM baseline_variables_stacked
)

SELECT * FROM t1 WHERE
-- treated_ever = 1
-- AND day = 1
-- AND paxlovid_treatment = 0
person_id = '1005991731621945870'

