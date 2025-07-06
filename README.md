# 📊 User Events Metrics Pipeline

🗓️ **Дата обновления**: 06.07.2025

## 📌 Цель задачи

Реализовать OLAP-пайплайн в ClickHouse для анализа пользовательских событий:

- Хранение сырых событий в таблице `user_events` (TTL 30 дней)
- Агрегирование данных в `user_daily_agg` (TTL 180 дней)
- Обновление агрегатов через `Materialized View`
- Поддержка метрики Retention 7d
- Быстрая аналитика по дням

---

## 🧱 Структура ClickHouse

### 1. Таблица сырых событий

```sql
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spend UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;
```

### 2. Агрегированная таблица

```sql
CREATE TABLE user_daily_agg (
    event_date DateTime,
    event_type String,
    uniq_user_state AggregateFunction(uniq, UInt32),
    points_spend_state AggregateFunction(sum, UInt32),
    action_count_state AggregateFunction(count, UInt8)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;
```

### 3. Materialized View

```sql
CREATE MATERIALIZED VIEW my_user_daily_agg
TO user_daily_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS uniq_user_state,
    sumState(points_spend) AS points_spend_state,
    countState() AS action_count_state
FROM user_events
GROUP BY event_date, event_type;
```

---

## 🧪 Тестовые данные

```sql
INSERT INTO user_events VALUES
-- События 10 дней назад
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

-- События 7 дней назад
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

-- События 5 дней назад
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

-- и т.д.
(6, 'purchase', 100, now());
```

---

## 📈 Быстрая аналитика по дням

```sql
SELECT
    event_date,
    event_type,
    uniqMerge(uniq_user_state) AS unique_users,
    sumMerge(points_spend_state) AS total_spend,
    countMerge(action_count_state) AS total_actions
FROM user_daily_agg
GROUP BY event_date, event_type
ORDER BY event_date;
```

---

## 🔁 Retention-метрика (7 дней)

```sql
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
LEFT JOIN returners r ON fe.user_id = r.user_id;
```

---

## 🔍 Альтернативная аналитика напрямую по `user_events`

```sql
SELECT
    toDate(event_time) AS event_date,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(points_spend) AS total_spend,
    COUNT(*) AS total_actions
FROM user_events
GROUP BY event_date, event_type
ORDER BY event_date DESC;
```

---

## 🧩 Функциональные особенности

- **Хранение истории**: TTL 30 дней для `user_events`, 180 дней для `user_daily_agg`.
- **State-функции**: `uniqState`, `sumState`, `countState`.
- **Merge-функции**: `uniqMerge`, `sumMerge`, `countMerge`.
- **Materialized View**: инкрементальное обновление агрегатов.
- **Retention**: отслеживание возвращаемости пользователей по first_event.

---

## ✅ Ожидаемый результат

Пример результата по агрегации:

| event_date | event_type | unique_users | total_spend | total_actions |
| ---------- | ---------- | ------------ | ----------- | ------------- |
| 2025-06-25 | purchase   | 3            | 150         | 3             |
| 2025-06-26 | login      | 4            | 0           | 4             |

Пример результата по Retention:

| total_users_day_0 | returned_in_7_days | retention_7d_percent |
| ----------------- | ------------------ | -------------------- |
| 10                | 7                  | 70.0                 |
