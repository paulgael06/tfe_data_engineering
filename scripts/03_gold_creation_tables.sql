/* GOLD_TFE - création des tables (modèle 1). Version de Paul, corrigée,
   + skills.skill_type et fact_job.salary_in_usd (16/10) :
   - virgule manquante dans fact_salary
   - BEGIN TRANSACTION / ROLLBACK retirés (le ROLLBACK annulait la création)
   - USE TFE_Gold ajouté */
USE TFE_Gold;
GO

/* --------------------------- Suppression ----------------------------- */
DROP TABLE IF EXISTS dbo.etl_load_log;
DROP TABLE IF EXISTS dbo.bridge;
DROP TABLE IF EXISTS dbo.Fact_job;
DROP TABLE IF EXISTS dbo.Fact_salary;
DROP TABLE IF EXISTS dbo.skills;
DROP TABLE IF EXISTS dbo.dim_job;
DROP TABLE IF EXISTS dbo.Dim_company;
DROP TABLE IF EXISTS dbo.Junk_dim;
DROP TABLE IF EXISTS dbo.Dim_country;
DROP TABLE IF EXISTS dbo.Dim_date;
GO

/* ----------------------------- DIMENSIONS ---------------------------- */

/* Dim_date : date_id = year_month du Silver (entier AAAAMM, ex. 202608).
   Pour Kaggle (année seule) : AAAA00 avec month = 0 (mois inconnu). */
CREATE TABLE dim_date (
    date_id  INT NOT NULL,
    year    INT NOT NULL,
    month   INT NOT NULL,      -- 1 à 12 ; 0 = mois inconnu (grain annuel)
    CONSTRAINT PK_Dim_date PRIMARY KEY (date_id),
    CONSTRAINT CK_Dim_date_month CHECK (month BETWEEN 0 AND 12)
);
GO

/* Dim_country : codes ISO alpha-2 de la table country du Silver
   + un membre 'ND' (chargé par le script Python). */
CREATE TABLE dim_country (
    country_code  VARCHAR(2)     NOT NULL,
    country_name  NVARCHAR(200)  NOT NULL,
    region        NVARCHAR(100)  NOT NULL,
    sub_region    NVARCHAR(100)  NOT NULL,
    CONSTRAINT PK_Dim_country PRIMARY KEY (country_code)
);
GO

/* Junk_dim : séniorité (vocabulaire commun aux deux sources),
   catégorie de métier, niveau d'études. */
CREATE TABLE junk_dim (
    id_Junk_dim   INT IDENTITY(1,1) NOT NULL,
    seniority     VARCHAR(20)   NOT NULL,
    job_category  VARCHAR(50)   NOT NULL,
    education_lvl VARCHAR(50)   NOT NULL,
    CONSTRAINT PK_Junk_dim PRIMARY KEY (id_Junk_dim),
    CONSTRAINT UQ_Junk_dim UNIQUE (seniority, job_category, education_lvl)
);
GO

CREATE TABLE dim_company (
    company_id    INT IDENTITY(1,1) NOT NULL,
    company_name      NVARCHAR(300)  NOT NULL,
    company_location  NVARCHAR(300)  NOT NULL,   -- 'ND' si inconnue
    company_size      NVARCHAR(50)   NOT NULL,
    domain        NVARCHAR(500)  NOT NULL,
    CONSTRAINT PK_Dim_company PRIMARY KEY (company_id)
);
GO

/* dim_job : une ligne par offre (Id_dim_job = ID du Silver, unique). */
CREATE TABLE dim_job (
    id_dim_job    INT             NOT NULL,
    title         NVARCHAR(500)   NULL,
    url           NVARCHAR(2000)  NULL,
    [source]      NVARCHAR(100)   NULL,
    [description] NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_dim_job PRIMARY KEY (id_dim_job)
);
GO

CREATE TABLE skills (
    id_skills    INT            NOT NULL,
    skills_name  NVARCHAR(200)  NOT NULL,
    skill_type   VARCHAR(10)    NOT NULL,   -- 'tool', 'concept' ou 'none' (noskill)
    CONSTRAINT PK_skills PRIMARY KEY (id_skills),
    CONSTRAINT UQ_skills_name UNIQUE (skills_name)
);
GO

/* ------------------------------- FAITS -------------------------------- */

/* Fact_job : grain = une ligne par offre x pays.
   job_id (PK) = Id_job_country du Silver : unique pour chaque ligne.
   Id_dim_job  = ID de l'offre : répété quand l'offre a plusieurs pays. */
CREATE TABLE fact_job (
    job_id               INT           NOT NULL,
    external_id          NVARCHAR(200) NULL,
    public_slug          NVARCHAR(500) NULL,
    Id_dim_job           INT           NOT NULL,
    date_id              INT           NOT NULL,
    company_id           INT           NOT NULL,
    country_code         VARCHAR(2)    NOT NULL,
    Id_Junk_dim          INT           NOT NULL,
    salary_min_annual    BIGINT        NULL,
    salary_max_annual    BIGINT        NULL,
    salary_ref_annual    BIGINT        NULL,
    salary_in_usd        BIGINT        NULL,   -- salary_ref_annual converti au taux du jour
    salary_currency      VARCHAR(10)   NOT NULL,   -- 'ND' si inconnue
    load_date            DATETIME2     NOT NULL,   -- date de chargement (ou de dernière mise à jour) de la ligne
    CONSTRAINT PK_Fact_job PRIMARY KEY (job_id),
    CONSTRAINT FK_Fact_job_dim_job  FOREIGN KEY (id_dim_job)   REFERENCES dim_job (id_dim_job),
    CONSTRAINT FK_Fact_job_date     FOREIGN KEY (date_id)      REFERENCES Dim_date (date_id),
    CONSTRAINT FK_Fact_job_company  FOREIGN KEY (company_id)   REFERENCES Dim_company (company_id),
    CONSTRAINT FK_Fact_job_country  FOREIGN KEY (country_code) REFERENCES Dim_country (country_code),
    CONSTRAINT FK_Fact_job_junk     FOREIGN KEY (id_Junk_dim)  REFERENCES Junk_dim (id_Junk_dim),
    /* une offre n'apparaît qu'une fois par pays */
    CONSTRAINT UQ_Fact_job_offre_pays UNIQUE (Id_dim_job, country_code)
);
GO

/* Fact_salary : grain = un salaire individuel Kaggle.
   Pays = employee_residence ; date = work_year relié à Dim_date (AAAA00). */
CREATE TABLE fact_salary (
    salary_id        INT          NOT NULL,
    date_id          INT          NOT NULL,
    country_code     VARCHAR(2)   NOT NULL,
    Id_Junk_dim      INT          NOT NULL,
    salary           BIGINT       NOT NULL,   -- montant en devise d'origine
    salary_in_usd    BIGINT       NULL,       -- montant en dollars (déjà converti par la source Kaggle)
    salary_currency  VARCHAR(10)  NOT NULL,
    CONSTRAINT PK_Fact_salary PRIMARY KEY (salary_id),
    CONSTRAINT FK_Fact_salary_date    FOREIGN KEY (date_id)      REFERENCES Dim_date (date_id),
    CONSTRAINT FK_Fact_salary_country FOREIGN KEY (country_code) REFERENCES Dim_country (country_code),
    CONSTRAINT FK_Fact_salary_junk    FOREIGN KEY (id_Junk_dim)  REFERENCES Junk_dim (id_Junk_dim)
);
GO

/* Table pont compétences : reliée UNIQUEMENT à Fact_job (job_id = clé de
   la ligne offre x pays). Les compétences d'une offre multi-pays sont donc
   répétées pour chaque pays. Pour compter des OFFRES : COUNT(DISTINCT Id_dim_job). */
CREATE TABLE bridge (
    Id_bridge  INT NOT NULL,
    Id_skills  INT NOT NULL,
    job_id     INT NOT NULL,
    CONSTRAINT PK_bridge PRIMARY KEY (id_bridge),
    CONSTRAINT UQ_bridge UNIQUE (job_id, id_skills),
    CONSTRAINT FK_bridge_skills   FOREIGN KEY (Id_skills) REFERENCES skills (id_skills),
    CONSTRAINT FK_bridge_Fact_job FOREIGN KEY (job_id)    REFERENCES fact_job (job_id)
);
GO


/* Journal des chargements (chargement complet et incrémentaux).
   watermark = plus grande date silver_loaded_at déjà reprise dans le Gold :
   le chargement incrémental ne lit que les lignes Silver plus récentes. */
CREATE TABLE etl_load_log (
    run_id            INT IDENTITY(1,1) NOT NULL,
    run_start         DATETIME2   NOT NULL,
    run_end           DATETIME2   NOT NULL,
    load_mode         VARCHAR(12) NOT NULL,   -- 'full' ou 'incremental'
    offers_new        INT         NOT NULL,
    offers_updated    INT         NOT NULL,
    fact_rows_loaded  INT         NOT NULL,
    bridge_rows_loaded INT        NOT NULL,
    watermark         DATETIME2   NULL,
    CONSTRAINT PK_etl_load_log PRIMARY KEY (run_id)
);
GO

/* Contrôle : liste des tables créées */
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME;
GO