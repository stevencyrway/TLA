
SET YEARS_BACK = 4;
SET YEARS_FORWARD = 1;

CREATE OR REPLACE TABLE FIVETRAN_DB.PROD.DATE_DIMENSION (
   DATE             DATE        NOT NULL
  ,YEAR             SMALLINT    NOT NULL
  ,MONTH            SMALLINT    NOT NULL
  ,MONTH_NAME       CHAR(3)     NOT NULL
  ,DAY_OF_MON       SMALLINT    NOT NULL
  ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
  ,WEEK_OF_YEAR     SMALLINT    NOT NULL
  ,DAY_OF_YEAR      SMALLINT    NOT NULL
)

AS

WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, -SEQ4(), dateadd(YEAR,$YEARS_FORWARD, current_date)) AS DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>(dayofyear(current_date())+($YEARS_BACK*365))))
  )
  SELECT DATE
        ,YEAR(DATE)
        ,MONTH(DATE)
        ,MONTHNAME(DATE)
        ,DAY(DATE)
        ,DAYOFWEEK(DATE)
        ,WEEKOFYEAR(DATE)
        ,DAYOFYEAR(DATE)
    FROM CTE_MY_DATE;