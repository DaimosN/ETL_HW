from faker import Faker
from pymongo import MongoClient
import uuid
import random
from datetime import datetime, timedelta
import json

fake = Faker()

# MongoDB connection
client = MongoClient(
    "mongodb://admin:admin@mongodb:27017/",
    authSource='admin'
)
db = client.analytics


def generate_user_sessions(n=1000):
    """Generate test data for UserSessions collection"""
    sessions = []
    for _ in range(n):
        start_time = fake.date_time_this_year()
        sessions.append({
            "session_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "start_time": start_time,
            "end_time": start_time + timedelta(minutes=random.randint(1, 120)),
            "pages_visited": [fake.uri_path() for _ in range(random.randint(1, 10))],
            "device": fake.user_agent(),
            "actions": [fake.word() for _ in range(random.randint(1, 5))],
            "last_modified": datetime.utcnow()
        })
    db.UserSessions.insert_many(sessions)
    print(f"Generated {n} UserSessions")


def generate_product_price_history(n=500):
    """Generate test data for ProductPriceHistory collection"""
    products = []
    for _ in range(n):
        price_changes = []
        for _ in range(random.randint(1, 5)):
            price_changes.append({
                "date": fake.date_time_this_year(),
                "price": round(random.uniform(10, 1000), 2)
            })

        products.append({
            "product_id": str(uuid.uuid4()),
            "price_changes": price_changes,
            "current_price": price_changes[-1]["price"],
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "last_modified": datetime.utcnow()
        })
    db.ProductPriceHistory.insert_many(products)
    print(f"Generated {n} ProductPriceHistory")


def generate_event_logs(n=2000):
    """Generate test data for EventLogs collection"""
    events = []
    for _ in range(n):
        events.append({
            "event_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_this_year(),
            "event_type": random.choice(["login", "purchase", "view", "logout"]),
            "details": {
                "ip": fake.ipv4(),
                "user_agent": fake.user_agent(),
                "location": fake.country_code()
            },
            "last_modified": datetime.utcnow()
        })
    db.EventLogs.insert_many(events)
    print(f"Generated {n} EventLogs")


def generate_support_tickets(n=300):
    """Generate test data for SupportTickets collection"""
    tickets = []
    for _ in range(n):
        created_at = fake.date_time_this_year()
        tickets.append({
            "ticket_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["technical", "billing", "general"]),
            "messages": [fake.sentence() for _ in range(random.randint(1, 5))],
            "created_at": created_at,
            "updated_at": created_at + timedelta(hours=random.randint(1, 72)),
            "last_modified": datetime.utcnow()
        })
    db.SupportTickets.insert_many(tickets)
    print(f"Generated {n} SupportTickets")


def generate_user_recommendations(n=1000):
    """Generate test data for UserRecommendations collection"""
    recommendations = []
    for _ in range(n):
        recommendations.append({
            "user_id": str(uuid.uuid4()),
            "recommended_products": [str(uuid.uuid4()) for _ in range(random.randint(3, 10))],
            "last_updated": fake.date_time_this_month(),
            "last_modified": datetime.utcnow()
        })
    db.UserRecommendations.insert_many(recommendations)
    print(f"Generated {n} UserRecommendations")


def generate_moderation_queue(n=400):
    """Generate test data for ModerationQueue collection"""
    reviews = []
    for _ in range(n):
        reviews.append({
            "review_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "product_id": str(uuid.uuid4()),
            "review_text": fake.text(max_nb_chars=200),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": random.choices(
                ["spam", "inappropriate", "fake", "offensive"],
                k=random.randint(0, 2)
            ),
            "submitted_at": fake.date_time_this_year(),
            "last_modified": datetime.utcnow()
        })
    db.ModerationQueue.insert_many(reviews)
    print(f"Generated {n} ModerationQueue")


def generate_search_queries(n=1500):
    """Generate test data for SearchQueries collection"""
    queries = []
    for _ in range(n):
        queries.append({
            "query_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "query_text": fake.sentence(nb_words=4).rstrip('.'),
            "timestamp": fake.date_time_this_month(),
            "filters": {
                "category": random.choice(["electronics", "books", "clothing"]),
                "price_min": random.randint(10, 100),
                "price_max": random.randint(101, 500)
            },
            "results_count": random.randint(0, 500),
            "last_modified": datetime.utcnow()
        })
    db.SearchQueries.insert_many(queries)
    print(f"Generated {n} SearchQueries")


def generate_all_data():
    """Generate test data for all collections"""
    generate_user_sessions()
    generate_product_price_history()
    generate_event_logs()
    generate_support_tickets()
    generate_user_recommendations()
    generate_moderation_queue()
    generate_search_queries()
    print("All test data generated successfully!")


if __name__ == "__main__":
    generate_all_data()
