CREATE TABLE events (
    user_id UInt32,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time;

CREATE TABLE event_counts (
    event_type String,
    count UInt32
) ENGINE = SummingMergeTree()
ORDER BY event_type;


CREATE MATERIALIZED VIEW mv_event_counts
TO event_counts AS
SELECT
    event_type,
    count() AS count
FROM events
GROUP BY event_type;


INSERT INTO events VALUES (1, 'click', now()), (2, 'view', now()), (3, 'click', now());


CREATE TABLE user_events(
 user_id UInt32,
 event_type String,
 points_spend UInt32,
 event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time,user_id)
TTL event_time + INTERVAL 30 DAY;


CREATE TABLE user_daily_agg(
event_date DateTime,
event_type String,
uniq_user_state AggregateFunction(uniq,UInt32),
points_spend_state AggregateFunction(sum,UInt32),
action_count_state AggregateFunction(count,UInt8)
)
ENGINE  = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


CREATE MATERIALIZED VIEW my_user_daily_agg
TO user_daily_agg 
AS
SELECT 
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS uniq_user_state,
    sumState(points_spend) AS points_spend_state,
    countState() AS action_count_state
FROM user_events
GROUP BY event_date, event_type;


INSERT INTO default.user_events (user_id, event_type, points_spend, event_time) VALUES
(1, 'click', 0, now() - INTERVAL 25 DAY),
(2, 'view', 0, now() - INTERVAL 24 DAY),
(3, 'purchase', 150, now() - INTERVAL 23 DAY),
(4, 'login', 0, now() - INTERVAL 22 DAY),
(5, 'scroll', 0, now() - INTERVAL 21 DAY),
(6, 'click', 20, now() - INTERVAL 20 DAY),
(7, 'view', 0, now() - INTERVAL 19 DAY),
(8, 'purchase', 300, now() - INTERVAL 18 DAY),
(9, 'login', 0, now() - INTERVAL 17 DAY),
(10, 'scroll', 0, now() - INTERVAL 16 DAY),
(1, 'view', 0, now() - INTERVAL 15 DAY),
(2, 'purchase', 100, now() - INTERVAL 14 DAY),
(3, 'click', 0, now() - INTERVAL 13 DAY),
(4, 'scroll', 0, now() - INTERVAL 12 DAY),
(5, 'login', 0, now() - INTERVAL 11 DAY),
(6, 'purchase', 200, now() - INTERVAL 10 DAY),
(7, 'click', 0, now() - INTERVAL 9 DAY),
(8, 'view', 0, now() - INTERVAL 8 DAY),
(9, 'scroll', 0, now() - INTERVAL 7 DAY),
(10, 'purchase', 50, now() - INTERVAL 6 DAY),
(1, 'login', 0, now() - INTERVAL 5 DAY),
(2, 'click', 0, now() - INTERVAL 4 DAY),
(3, 'view', 0, now() - INTERVAL 3 DAY),
(4, 'purchase', 75, now() - INTERVAL 2 DAY),
(5, 'scroll', 0, now() - INTERVAL 1 DAY),
(6, 'click', 0, now()),
(7, 'login', 0, now() - INTERVAL 1 DAY + INTERVAL 1 HOUR),
(8, 'purchase', 120, now() - INTERVAL 2 DAY + INTERVAL 3 HOUR),
(9, 'view', 0, now() - INTERVAL 3 DAY + INTERVAL 4 HOUR),
(10, 'scroll', 0, now() - INTERVAL 4 DAY + INTERVAL 5 HOUR),
(1, 'purchase', 90, now() - INTERVAL 5 DAY + INTERVAL 6 HOUR),
(2, 'click', 0, now() - INTERVAL 6 DAY + INTERVAL 7 HOUR),
(3, 'login', 0, now() - INTERVAL 7 DAY + INTERVAL 8 HOUR),
(4, 'view', 0, now() - INTERVAL 8 DAY + INTERVAL 9 HOUR),
(5, 'scroll', 0, now() - INTERVAL 9 DAY + INTERVAL 10 HOUR),
(6, 'purchase', 200, now() - INTERVAL 10 DAY + INTERVAL 11 HOUR),
(7, 'click', 0, now() - INTERVAL 11 DAY + INTERVAL 12 HOUR),
(8, 'login', 0, now() - INTERVAL 12 DAY + INTERVAL 13 HOUR),
(9, 'purchase', 180, now() - INTERVAL 13 DAY + INTERVAL 14 HOUR),
(10, 'view', 0, now() - INTERVAL 14 DAY + INTERVAL 15 HOUR),
(1, 'scroll', 0, now() - INTERVAL 15 DAY + INTERVAL 16 HOUR),
(2, 'click', 0, now() - INTERVAL 16 DAY + INTERVAL 17 HOUR),
(3, 'purchase', 250, now() - INTERVAL 17 DAY + INTERVAL 18 HOUR),
(4, 'login', 0, now() - INTERVAL 18 DAY + INTERVAL 19 HOUR),
(5, 'view', 0, now() - INTERVAL 19 DAY + INTERVAL 20 HOUR),
(6, 'scroll', 0, now() - INTERVAL 20 DAY + INTERVAL 21 HOUR),
(7, 'purchase', 300, now() - INTERVAL 21 DAY + INTERVAL 22 HOUR),
(8, 'click', 0, now() - INTERVAL 22 DAY + INTERVAL 23 HOUR),
(9, 'login', 0, now() - INTERVAL 23 DAY + INTERVAL 20 MINUTE),
(10, 'view', 0, now() - INTERVAL 24 DAY + INTERVAL 40 MINUTE);


SELECT uniq(uniq_user_state) FROM my_user_daily_agg;

SELECT event_date FROM my_user_daily_agg

SELECT 
    event_date,
    event_type,
    uniqMerge(uniq_user_state) AS unique_users,
    sumMerge(points_spend_state) AS total_spend,
    countMerge(action_count_state) AS total_actions
FROM user_daily_agg uda 
GROUP BY event_date, event_type
ORDER BY event_date;



WITH
first_events AS (
    SELECT
        user_id,
        toDate(min(event_time)) AS first_event_date
    FROM user_events
    GROUP BY user_id
),
returners AS (
    SELECT
        fe.user_id,
        fe.first_event_date
    FROM first_events fe
    INNER JOIN user_events ue
        ON fe.user_id = ue.user_id
        AND toDate(ue.event_time) > fe.first_event_date
        AND toDate(ue.event_time) <= fe.first_event_date + INTERVAL 7 DAY
    GROUP BY fe.user_id, fe.first_event_date
)
SELECT
    COUNT(DISTINCT fe.user_id) AS total_users_day_0,
    COUNT(DISTINCT r.user_id) AS returned_in_7_days,
    round(
        (COUNT(DISTINCT r.user_id) / COUNT(DISTINCT fe.user_id)) * 100,
        1
    ) AS retention_7d_percent
FROM first_events fe
LEFT JOIN returners r
    ON fe.user_id = r.user_id;



SELECT
    toDate(event_time) AS event_date,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(points_spend) AS total_spend,
    COUNT(*) AS total_actions
FROM default.user_events
GROUP BY
    event_date,
    event_type
ORDER BY
    event_date DESC,
    event_type;

