/* Contrôles du Gold (TFE_Gold) - à lancer après le chargement.
   Envoyer les résultats de chaque requête pour validation. */
USE TFE_Gold;
GO

/* 1. Nombre de lignes par table
      attendu : fact_job 49377, fact_salary 71913, dim_job 47410 */
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_country', COUNT(*) FROM dim_country
UNION ALL SELECT 'junk_dim', COUNT(*) FROM junk_dim
UNION ALL SELECT 'dim_company', COUNT(*) FROM dim_company
UNION ALL SELECT 'dim_job', COUNT(*) FROM dim_job
UNION ALL SELECT 'skills', COUNT(*) FROM skills
UNION ALL SELECT 'fact_job', COUNT(*) FROM fact_job
UNION ALL SELECT 'fact_salary', COUNT(*) FROM fact_salary
UNION ALL SELECT 'bridge', COUNT(*) FROM bridge;

/* 2. Offres distinctes (attendu : 47410) */
SELECT COUNT(DISTINCT Id_dim_job) AS distinct_offers FROM fact_job;

/* 3. Types de compétences */
SELECT skill_type, COUNT(*) AS skills_count FROM skills GROUP BY skill_type;

/* 4. Couverture du salaire en dollars (freehire) */
SELECT
    COUNT(*)                                                       AS rows_total,
    SUM(CASE WHEN salary_ref_annual IS NOT NULL THEN 1 ELSE 0 END) AS with_ref_salary,
    SUM(CASE WHEN salary_in_usd IS NOT NULL THEN 1 ELSE 0 END)     AS with_usd_salary
FROM fact_job;

/* 5. Devises sans conversion alors qu'un montant existe */
SELECT salary_currency, COUNT(*) AS rows_without_usd
FROM fact_job
WHERE salary_ref_annual IS NOT NULL AND salary_in_usd IS NULL
GROUP BY salary_currency
ORDER BY rows_without_usd DESC;

/* 6. Valeurs extrêmes en dollars (détection d'anomalies) */
SELECT 'freehire' AS source,
       MIN(salary_in_usd) AS min_usd, MAX(salary_in_usd) AS max_usd,
       SUM(CASE WHEN salary_in_usd < 10000 THEN 1 ELSE 0 END)   AS below_10k,
       SUM(CASE WHEN salary_in_usd > 1000000 THEN 1 ELSE 0 END) AS above_1m
FROM fact_job
UNION ALL
SELECT 'kaggle',
       MIN(salary_in_usd), MAX(salary_in_usd),
       SUM(CASE WHEN salary_in_usd < 10000 THEN 1 ELSE 0 END),
       SUM(CASE WHEN salary_in_usd > 1000000 THEN 1 ELSE 0 END)
FROM fact_salary;
GO
