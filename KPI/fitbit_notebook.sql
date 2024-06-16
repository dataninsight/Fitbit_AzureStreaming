-- Databricks notebook source
select * from dev.sbit-managed-dev.dim_bpm limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.dim_gym_logins limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.dim_registered_users limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.dim_users limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.dim_workout limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.fact_gym limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.fact_workout limit 3

-- COMMAND ----------

select * from dev.sbit-managed-dev.lookup_date limit 3

-- COMMAND ----------

-- DBTITLE 1,Average Heart Rate per User
SELECT 
    ru.user_id,
    u.first_name,
    u.last_name,
    AVG(bpm.heartrate) AS avg_heart_rate
FROM 
    dev.sbit-managed-dev.dim_bpm bpm
JOIN 
    dev.sbit-managed-dev.dim_registered_users ru ON bpm.device_id = ru.device_id
JOIN 
    dev.sbit-managed-dev.dim_users u ON ru.user_id = u.user_id
GROUP BY 
    ru.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,Averageheartbeatof user
SELECT 
    ru.user_id,
    u.first_name,
    u.last_name,
    AVG(bpm.heartrate) AS avg_heart_rate
FROM 
    dev.sbit-managed-dev.dim_bpm bpm
JOIN 
    dev.sbit-managed-dev.dim_registered_users ru ON bpm.device_id = ru.device_id
JOIN 
    dev.sbit-managed-dev.dim_users u ON ru.user_id = u.user_id
GROUP BY 
    ru.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,total gym_time per user
SELECT 
    ru.user_id,
    u.first_name,
    u.last_name,
    SUM(fg.minutes_in_gym) AS total_gym_time
FROM 
    dev.sbit-managed-dev.fact_gym fg
JOIN 
    dev.sbit-managed-dev.dim_registered_users ru ON fg.mac_address = ru.mac_address
JOIN 
    dev.sbit-managed-dev.dim_users u ON ru.user_id = u.user_id
GROUP BY 
    ru.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,total_exercise_time_per_user
SELECT 
    ru.user_id,
    u.first_name,
    u.last_name,
    SUM(fg.minutes_exercising) AS total_exercise_time
FROM 
    dev.sbit-managed-dev.fact_gym fg
JOIN 
    dev.sbit-managed-dev.dim_registered_users ru ON fg.mac_address = ru.mac_address
JOIN 
    dev.sbit-managed-dev.dim_users u ON ru.user_id = u.user_id
GROUP BY 
    ru.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,number of gym_logins per user
SELECT 
    ru.user_id,
    u.first_name,
    u.last_name,
    COUNT(dg.gym) AS number_of_logins
FROM 
    dev.sbit-managed-dev.dim_gym_logins dg
JOIN 
    dev.sbit-managed-dev.dim_registered_users ru ON dg.mac_address = ru.mac_address
JOIN 
    dev.sbit-managed-dev.dim_users u ON ru.user_id = u.user_id
GROUP BY 
    ru.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,workout session per user
SELECT 
    w.user_id,
    u.first_name,
    u.last_name,
    COUNT(w.workout_id) AS total_workout_sessions
FROM 
    dev.sbit-managed-dev.dim_workout w
JOIN 
    dev.sbit-managed-dev.dim_users u ON w.user_id = u.user_id
GROUP BY 
    w.user_id, u.first_name, u.last_name;

-- COMMAND ----------

-- DBTITLE 1,Average minimum and maximum BPM per workout
SELECT 
    fw.workout_id,
    fw.session_id,
    fw.user_id,
    fw.age,
    fw.gender,
    fw.city,
    fw.state,
    fw.min_bpm,
    fw.avg_bpm,
    fw.max_bpm,
    fw.num_recordings
FROM 
    dev.sbit-managed-dev.fact_workout fw;

-- COMMAND ----------

-- DBTITLE 1,age_distribution of user
SELECT 
    fw.age,
    COUNT(DISTINCT fw.user_id) AS number_of_users
FROM 
    dev.sbit-managed-dev.fact_workout fw
GROUP BY 
    fw.age
ORDER BY 
    fw.age;

-- COMMAND ----------

-- DBTITLE 1,gender_distribution
SELECT 
    u.gender,
    COUNT(DISTINCT u.user_id) AS number_of_users
FROM 
    dev.sbit-managed-dev.dim_users u
GROUP BY 
    u.gender;

-- COMMAND ----------

-- DBTITLE 1,city distribution
SELECT 
    u.city,
    COUNT(DISTINCT u.user_id) AS number_of_users
FROM 
    dev.sbit-managed-dev.dim_users u
GROUP BY 
    u.city
ORDER BY 
    number_of_users DESC;

-- COMMAND ----------

WITH RecentUserUpdates AS (
    SELECT
        user_id,
        dob,
        MAX(timestamp) AS recent_timestamp
    FROM
        dev.sbit-managed-dev.dim_users
    WHERE
        update_type = 'update'
    GROUP BY
        user_id, dob
)
SELECT 
    CASE
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 0 AND 9 THEN '0-9'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 10 AND 19 THEN '10-19'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 20 AND 29 THEN '20-29'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 30 AND 39 THEN '30-39'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 40 AND 49 THEN '40-49'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 50 AND 59 THEN '50-59'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 60 AND 69 THEN '60-69'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 70 AND 79 THEN '70-79'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 80 AND 89 THEN '80-89'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 90 AND 99 THEN '90-99'
        ELSE '100+'
    END AS age_group,
    COUNT(DISTINCT ru.user_id) AS number_of_users
FROM 
    RecentUserUpdates ru
GROUP BY 
    age_group
ORDER BY 
    age_group;

-- COMMAND ----------

WITH RankedUsers AS (
    SELECT
        user_id,
        dob,
        timestamp,
        update_type,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY 
                CASE 
                    WHEN update_type = 'update' THEN 1 
                    ELSE 2 
                END, 
                timestamp DESC
        ) AS rank
    FROM
        dev.sbit-managed-dev.dim_users
)
SELECT 
    CASE
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 0 AND 9 THEN '0-9'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 10 AND 19 THEN '10-19'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 20 AND 29 THEN '20-29'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 30 AND 39 THEN '30-39'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 40 AND 49 THEN '40-49'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 50 AND 59 THEN '50-59'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 60 AND 69 THEN '60-69'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 70 AND 79 THEN '70-79'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 80 AND 89 THEN '80-89'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 90 AND 99 THEN '90-99'
        ELSE '100+'
    END AS age_group,
    COUNT(DISTINCT ru.user_id) AS number_of_users
FROM 
    RankedUsers ru
WHERE 
    ru.rank = 1
GROUP BY 
    age_group
ORDER BY 
    age_group;

-- COMMAND ----------

WITH RankedUsers AS (
    SELECT
        user_id,
        dob,
        timestamp,
        update_type,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY 
                CASE 
                    WHEN update_type = 'update' THEN 1 
                    ELSE 2 
                END, 
                timestamp DESC
        ) AS rank
    FROM
        dev.sbit-managed-dev.dim_users
),
RecentUsers AS (
    SELECT 
        user_id,
        dob
    FROM 
        RankedUsers
    WHERE 
        rank = 1
)
SELECT 
    ru.user_id,
    MAX(CASE
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 0 AND 9 THEN '0-9'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 10 AND 19 THEN '10-19'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 20 AND 29 THEN '20-29'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 30 AND 39 THEN '30-39'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 40 AND 49 THEN '40-49'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 50 AND 59 THEN '50-59'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 60 AND 69 THEN '60-69'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 70 AND 79 THEN '70-79'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 80 AND 89 THEN '80-89'
        WHEN FLOOR(DATEDIFF(CURDATE(), ru.dob) / 365) BETWEEN 90 AND 99 THEN '90-99'
        ELSE '100+'
    END) AS age_group,
    MIN(bpm.heartrate) AS min_bpm,
    AVG(bpm.heartrate) AS avg_bpm,
    MAX(bpm.heartrate) AS max_bpm
FROM 
--     dim_workout w
-- JOIN 
    dim_registered_users r --ON w.user_id = r.user_id
JOIN 
    RecentUsers ru ON r.user_id = ru.user_id 
JOIN 
    dim_bpm bpm ON r.device_id = bpm.device_id
--WHERE 
   -- bpm.timestamp BETWEEN w.timestamp AND w.timestamp + 3600 -- assuming workout duration is 1 hour
GROUP BY 
    --w.user_id, w.workout_id, w.session_id;
    ru.user_id,r.device_id
