SELECT 1 AS stage_order, 'visit' AS event_name, COUNT(DISTINCT user_id) AS users
FROM user_events
WHERE event_name = 'visit'
UNION ALL
SELECT 2 AS stage_order, 'signup' AS event_name, COUNT(DISTINCT user_id) AS users
FROM user_events
WHERE event_name = 'signup'
UNION ALL
SELECT 3 AS stage_order, 'onboarding_complete' AS event_name, COUNT(DISTINCT user_id) AS users
FROM user_events
WHERE event_name = 'onboarding_complete'
UNION ALL
SELECT 4 AS stage_order, 'first_purchase' AS event_name, COUNT(DISTINCT user_id) AS users
FROM user_events
WHERE event_name = 'first_purchase'
ORDER BY stage_order;
