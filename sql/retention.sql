WITH RECURSIVE days AS (
    SELECT 0 AS day_number
    UNION ALL
    SELECT day_number + 1
    FROM days
    WHERE day_number < 7
),
signups AS (
    SELECT
        user_id,
        DATE(MIN(event_time)) AS cohort_date
    FROM user_events
    WHERE event_name = 'signup'
    GROUP BY user_id
),
activity AS (
    SELECT
        s.cohort_date,
        CAST(julianday(DATE(e.event_time)) - julianday(s.cohort_date) AS INTEGER) AS day_number,
        s.user_id
    FROM signups AS s
    JOIN user_events AS e
        ON e.user_id = s.user_id
    WHERE DATE(e.event_time) >= s.cohort_date
      AND CAST(julianday(DATE(e.event_time)) - julianday(s.cohort_date) AS INTEGER) BETWEEN 0 AND 7
),
cohort_size AS (
    SELECT
        cohort_date,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM signups
    GROUP BY cohort_date
)
SELECT
    c.cohort_date,
    d.day_number,
    COUNT(DISTINCT a.user_id) AS retained_users,
    c.cohort_size,
    ROUND(COUNT(DISTINCT a.user_id) * 100.0 / c.cohort_size, 2) AS retention_rate
FROM cohort_size AS c
CROSS JOIN days AS d
LEFT JOIN activity AS a
    ON a.cohort_date = c.cohort_date
   AND a.day_number = d.day_number
GROUP BY c.cohort_date, d.day_number, c.cohort_size
ORDER BY c.cohort_date, d.day_number;
