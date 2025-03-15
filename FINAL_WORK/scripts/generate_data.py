from faker import Faker
from pymongo import MongoClient
import uuid
import random
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_all_data():
    try:
        logger.info("Connecting to MongoDB...")
        client = MongoClient(
            "mongodb://admin:admin@mongodb:27017/",
            authSource='admin',
            serverSelectionTimeoutMS=5000
        )
        client.server_info()  # Проверка подключения
        db = client.analytics

        logger.info("Generating UserSessions...")
        generate_user_sessions()

        logger.info("Generating ProductPriceHistory...")
        generate_product_price_history()

        # Добавьте логи для других функций генерации...

        logger.info("Data generation completed successfully!")

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise


fake = Faker()

client = MongoClient(
    "mongodb://admin:admin@mongodb:27017/",
    authSource='admin'
)
db = client.analytics


def generate_user_sessions(n=1000):
    sessions = []
    for _ in range(n):
        start = fake.date_time_between(start_date="-1y", end_date="now")
        sessions.append({
            "session_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "start_time": start,
            "end_time": start + timedelta(minutes=random.randint(1, 120)),
            "pages_visited": [fake.uri_path() for _ in range(random.randint(1, 10))],
            "device": fake.user_agent(),
            "actions": [fake.word() for _ in range(random.randint(1, 5))]
        })
    db.UserSessions.insert_many(sessions)


def generate_product_price_history(n=500):
    products = []
    for _ in range(n):
        changes = []
        for _ in range(random.randint(1, 5)):
            changes.append({
                "date": fake.date_time_between(start_date="-1y", end_date="now"),  # Исправлено
                "price": round(random.uniform(10, 1000), 2)
            })
        products.append({
            "product_id": str(uuid.uuid4()),
            "price_changes": changes,
            "current_price": changes[-1]["price"],
            "currency": random.choice(["USD", "EUR", "GBP"])
        })
    db.ProductPriceHistory.insert_many(products)


def generate_event_logs(n=2000):
    events = []
    for _ in range(n):
        events.append({
            "event_id": str(uuid.uuid4()),
            "timestamp": fake.date_time_this_year(),
            "event_type": random.choice(["login", "purchase", "view", "logout"]),
            "details": {
                "ip": fake.ipv4(),
                "description": fake.sentence()
            }
        })
    db.EventLogs.insert_many(events)


def generate_support_tickets(n=300):
    tickets = []
    for _ in range(n):
        created = fake.date_time_this_year()
        tickets.append({
            "ticket_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["technical", "billing", "general"]),
            "messages": [fake.text() for _ in range(random.randint(1, 5))],
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(1, 72))
        })
    db.SupportTickets.insert_many(tickets)


def generate_user_recommendations(n=1000):
    recommendations = []
    for _ in range(n):
        recommendations.append({
            "user_id": str(uuid.uuid4()),
            "recommended_products": [str(uuid.uuid4()) for _ in range(random.randint(3, 10))],
            "last_updated": fake.date_time_this_month()
        })
    db.UserRecommendations.insert_many(recommendations)


def generate_moderation_queue(n=400):
    reviews = []
    for _ in range(n):
        reviews.append({
            "review_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "product_id": str(uuid.uuid4()),
            "review_text": fake.text(max_nb_chars=200),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": [random.choice(["spam", "inappropriate", "fake"]) for _ in range(random.randint(0, 3))],
            "submitted_at": fake.date_time_this_year()  # Исправлено
        })
    db.ModerationQueue.insert_many(reviews)


def generate_search_queries(n=1500):
    queries = []
    for _ in range(n):
        queries.append({
            "query_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "query_text": fake.sentence(nb_words=4),
            "timestamp": fake.date_time_this_month(),
            "filters": {
                "category": random.choice(["electronics", "books", "clothing"]),
                "price_range": f"{random.randint(10, 100)}-{random.randint(101, 500)}"
            },
            "results_count": random.randint(0, 500)
        })
    db.SearchQueries.insert_many(queries)


if __name__ == "__main__":
    generate_all_data()
    generate_user_sessions()
    generate_product_price_history()
    generate_event_logs()
    generate_support_tickets()
    generate_user_recommendations()
    generate_moderation_queue()
    generate_search_queries()
