CREATE DATABASE analytics;
\c analytics

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited JSONB,
    device TEXT,
    actions JSONB
);

CREATE TABLE IF NOT EXISTS product_price_history (
    product_id UUID PRIMARY KEY,
    price_changes JSONB,
    current_price NUMERIC(10,2),
    currency VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS event_logs (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type TEXT,
    details JSONB
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id UUID PRIMARY KEY,
    user_id UUID,
    status TEXT,
    issue_type TEXT,
    messages JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id UUID PRIMARY KEY,
    recommended_products JSONB,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id UUID PRIMARY KEY,
    user_id UUID,
    product_id UUID,
    review_text TEXT,
    rating INTEGER,
    moderation_status TEXT,
    flags JSONB,
    submitted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS search_queries (
    query_id UUID PRIMARY KEY,
    user_id UUID,
    query_text TEXT,
    timestamp TIMESTAMP,
    filters JSONB,
    results_count INTEGER
);
