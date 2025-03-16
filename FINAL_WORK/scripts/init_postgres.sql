-- Создание основной базы данных
CREATE DATABASE analytics;
\c analytics;

-- Таблица: Сессии пользователей
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_visited JSONB NOT NULL,
    device TEXT NOT NULL,
    actions JSONB NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_user_sessions_last_modified ON user_sessions(last_modified);

-- Таблица: История цен продуктов
CREATE TABLE IF NOT EXISTS product_price_history (
    product_id UUID PRIMARY KEY,
    price_changes JSONB NOT NULL,
    current_price NUMERIC(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_product_prices_last_modified ON product_price_history(last_modified);

-- Таблица: Логи событий
CREATE TABLE IF NOT EXISTS event_logs (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details JSONB NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_event_logs_last_modified ON event_logs(last_modified);

-- Таблица: Обращения в поддержку
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    issue_type VARCHAR(20) NOT NULL,
    messages JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_support_tickets_last_modified ON support_tickets(last_modified);

-- Таблица: Рекомендации пользователей
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id UUID PRIMARY KEY,
    recommended_products JSONB NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_user_recommendations_last_modified ON user_recommendations(last_modified);

-- Таблица: Очередь модерации
CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    review_text TEXT NOT NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    moderation_status VARCHAR(20) NOT NULL,
    flags JSONB NOT NULL,
    submitted_at TIMESTAMP NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_moderation_queue_last_modified ON moderation_queue(last_modified);

-- Таблица: Поисковые запросы
CREATE TABLE IF NOT EXISTS search_queries (
    query_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    query_text TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    filters JSONB NOT NULL,
    results_count INTEGER NOT NULL,
    last_modified TIMESTAMP NOT NULL
);

CREATE INDEX idx_search_queries_last_modified ON search_queries(last_modified);

-- Материализованные представления

-- Витрина активности пользователей
CREATE MATERIALIZED VIEW IF NOT EXISTS user_activity AS
SELECT
    user_id,
    COUNT(*) AS total_sessions,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_session_duration,
    SUM(jsonb_array_length(pages_visited)) AS total_pages_viewed
FROM user_sessions
GROUP BY user_id;

-- Витрина эффективности поддержки
CREATE MATERIALIZED VIEW IF NOT EXISTS support_efficiency AS
SELECT
    status,
    issue_type,
    COUNT(*) AS ticket_count,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) AS avg_resolution_time
FROM support_tickets
GROUP BY status, issue_type;

-- Права доступа
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
