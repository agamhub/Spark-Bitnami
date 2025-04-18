CREATE OR REPLACE TEMPORARY VIEW temp_jobs AS
SELECT `Job Title`, COUNT(1) CNT
FROM PEOPLEPF
WHERE YEAR(`Date of birth`) IN ('1992', '1993', '1995', '2021')
GROUP BY `Job Title`;

CREATE OR REPLACE TEMPORARY VIEW temp_jobs_2 AS
SELECT `Job Title`, COUNT(1) CNT
FROM PEOPLEPF
WHERE YEAR(`Date of birth`) IN ('2021')
GROUP BY `Job Title`;

SELECT COUNT(1) CNT
FROM (
SELECT *
FROM temp_jobs
UNION ALL
SELECT *
FROM temp_jobs_2
);