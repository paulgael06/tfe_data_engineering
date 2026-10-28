/* Vues du Gold (TFE_Gold) pour Power BI : une vue par question métier.
   Ce sont des requêtes enregistrées : aucune table n'est modifiée. */
USE TFE_Gold;
GO

/* Q1 : Quels outils sont demandés par métier ?
   - seuls les outils sont gardés (skill_type = 'tool')
   - on compte des OFFRES distinctes (une offre multi-pays compte une fois)
   - pct_of_offers = part des offres du métier qui demandent l'outil
   - seuls les 10 outils les plus demandés de chaque métier sont renvoyés */
CREATE OR ALTER VIEW dbo.vw_q1_tools_by_job_category AS
WITH offers_by_category AS (
    SELECT j.job_category,
           COUNT(DISTINCT f.Id_dim_job) AS total_offers
    FROM fact_job f
    JOIN junk_dim j ON j.id_Junk_dim = f.Id_Junk_dim
    GROUP BY j.job_category
),
tool_offers AS (
    SELECT j.job_category,
           s.skills_name AS tool,
           COUNT(DISTINCT f.Id_dim_job) AS offers_count
    FROM fact_job f
    JOIN junk_dim j ON j.id_Junk_dim = f.Id_Junk_dim
    JOIN bridge b   ON b.job_id = f.job_id
    JOIN skills s   ON s.id_skills = b.Id_skills
    WHERE s.skill_type = 'tool'
    GROUP BY j.job_category, s.skills_name
),
ranked AS (
    SELECT t.job_category,
           t.tool,
           t.offers_count,
           o.total_offers,
           CAST(100.0 * t.offers_count / o.total_offers AS DECIMAL(5, 2)) AS pct_of_offers,
           /* rang = 1 + nombre d'outils du même métier qui sont plus demandés */
           (SELECT COUNT(*)
            FROM tool_offers t2
            WHERE t2.job_category = t.job_category
              AND t2.offers_count > t.offers_count) + 1 AS rank_in_category
    FROM tool_offers t
    JOIN offers_by_category o ON o.job_category = t.job_category
)
SELECT job_category, tool, offers_count, total_offers, pct_of_offers, rank_in_category
FROM ranked
WHERE rank_in_category <= 5;
GO

/* Q2 : Quel salaire par séniorité et par pays ?
   - une ligne par pays et par séniorité ; les deux sources côte à côte
     (colonnes freehire_... et kaggle_...)
   - salaire annuel en dollars (salary_in_usd), médiane car la moyenne est
     faussée par quelques salaires aberrants
   - seules les 4 séniorités présentes dans les deux sources sont gardées
     (junior, middle, senior, c_level) ; la séniorité inconnue (ND) est exclue
   - un groupe de moins de 30 salaires est masqué (médiane peu fiable) :
     la colonne reste alors vide (NULL)
   - seniority_order sert à trier du plus junior au plus senior
     (le tri se fait à la lecture, pas dans la vue) */
CREATE OR ALTER VIEW dbo.vw_q2_salary_usd_by_seniority_country AS
WITH salaries AS (
    SELECT 'freehire' AS source, j.seniority, f.country_code, f.salary_in_usd
    FROM fact_job f
    JOIN junk_dim j ON j.id_Junk_dim = f.Id_Junk_dim
    WHERE f.salary_in_usd IS NOT NULL
      AND j.seniority IN ('junior', 'middle', 'senior', 'c_level')
    UNION ALL
    SELECT 'kaggle', j.seniority, s.country_code, s.salary_in_usd
    FROM fact_salary s
    JOIN junk_dim j ON j.id_Junk_dim = s.Id_Junk_dim
    WHERE s.salary_in_usd IS NOT NULL
      AND j.seniority IN ('junior', 'middle', 'senior', 'c_level')
),
group_counts AS (
    SELECT source, seniority, country_code, COUNT(*) AS salaries_count
    FROM salaries
    GROUP BY source, seniority, country_code
    HAVING COUNT(*) >= 30
),
group_medians AS (
    SELECT DISTINCT source, seniority, country_code,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_in_usd)
               OVER (PARTITION BY source, seniority, country_code) AS median_salary_usd
    FROM salaries
),
by_source AS (
    SELECT g.source, g.seniority, g.country_code, g.salaries_count, m.median_salary_usd
    FROM group_counts g
    JOIN group_medians m ON m.source = g.source
                        AND m.seniority = g.seniority
                        AND m.country_code = g.country_code
)
SELECT b.country_code,
       c.country_name,
       b.seniority,
       CASE b.seniority WHEN 'junior' THEN 1 WHEN 'middle' THEN 2
                        WHEN 'senior' THEN 3 ELSE 4 END AS seniority_order,
       MAX(CASE WHEN b.source = 'freehire' THEN b.salaries_count END) AS freehire_count,
       MAX(CASE WHEN b.source = 'freehire'
                THEN CAST(b.median_salary_usd AS BIGINT) END)         AS freehire_median_usd,
       MAX(CASE WHEN b.source = 'kaggle' THEN b.salaries_count END)   AS kaggle_count,
       MAX(CASE WHEN b.source = 'kaggle'
                THEN CAST(b.median_salary_usd AS BIGINT) END)         AS kaggle_median_usd
FROM by_source b
JOIN dim_country c ON c.country_code = b.country_code
GROUP BY b.country_code, c.country_name, b.seniority;
GO

/* Contrôle rapide des deux vues */
SELECT * FROM dbo.vw_q1_tools_by_job_category WHERE rank_in_category <= 5
ORDER BY job_category, rank_in_category;
SELECT * FROM dbo.vw_q2_salary_usd_by_seniority_country
WHERE country_code NOT IN ('AR','LV', 'NZ', 'PH', 'SG','BR', 'MX','IE','PT')
ORDER BY country_name, seniority_order;
GO


SELECT * FROM dbo.etl_load_log