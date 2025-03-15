from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def json_serializer(obj: Any) -> str:
    """Custom JSON serializer for non-serializable objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}


def migrate_collection(collection_name: str, table_name: str, transform_fn: callable):
    def _migrate():
        try:
            logger.info(f"Starting migration for {collection_name}")

            # MongoDB connection
            mongo_client = MongoClient(
                "mongodb://admin:admin@mongodb:27017/",
                authSource='admin',
                serverSelectionTimeoutMS=10000
            )
            mongo_db = mongo_client.analytics
            mongo_col = mongo_db[collection_name]

            # PostgreSQL connection
            conn = psycopg2.connect(
                dbname="analytics",
                user="airflow",
                password="airflow",
                host="postgres"
            )
            cur = conn.cursor()

            # Clear existing data
            cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")

            # Transfer data
            total = 0
            for doc in mongo_col.find():
                try:
                    data = transform_fn(doc)
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join(['%s'] * len(data))

                    query = f"""
                        INSERT INTO {table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    cur.execute(query, list(data.values()))
                    total += 1
                except Exception as e:
                    logger.error(f"Error in document {doc.get('_id')}: {str(e)}")
                    continue

            conn.commit()
            logger.info(f"Successfully migrated {total} records to {table_name}")

        except Exception as e:
            logger.error(f"Critical error: {str(e)}", exc_info=True)
            raise
        finally:
            conn.close()
            mongo_client.close()

    return _migrate


def create_analytical_views():
    conn = psycopg2.connect(
        dbname="analytics",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    # Refresh materialized views
    cur.execute("REFRESH MATERIALIZED VIEW user_activity")
    cur.execute("REFRESH MATERIALIZED VIEW support_efficiency")

    conn.commit()
    conn.close()


with DAG(
        'data_replication',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['data_pipeline']
) as dag:
    # 1. User Sessions
    replicate_user_sessions = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=migrate_collection(
            'UserSessions',
            'user_sessions',
            lambda doc: {
                'session_id': str(doc['session_id']),
                'user_id': str(doc['user_id']),
                'start_time': doc['start_time'].isoformat(),
                'end_time': doc['end_time'].isoformat(),
                'pages_visited': json.dumps(doc['pages_visited']),
                'device': str(doc['device']),
                'actions': json.dumps(doc['actions'])
            }
        )
    )

    # 2. Product Price History
    replicate_product_prices = PythonOperator(
        task_id='replicate_product_prices',
        python_callable=migrate_collection(
            'ProductPriceHistory',
            'product_price_history',
            lambda doc: {
                'product_id': str(doc['product_id']),
                'price_changes': json.dumps(
                    [
                        {
                            "date": change['date'].isoformat(),
                            "price": float(change['price'])
                        }
                        for change in doc['price_changes']
                    ],
                    default=json_serializer
                ),
                'current_price': float(doc['current_price']),
                'currency': str(doc['currency'])
            }
        )
    )

    # 3. Event Logs
    replicate_event_logs = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=migrate_collection(
            'EventLogs',
            'event_logs',
            lambda doc: {
                'event_id': str(doc['event_id']),
                'timestamp': doc['timestamp'].isoformat(),
                'event_type': str(doc['event_type']),
                'details': json.dumps(doc['details'], default=json_serializer)
            }
        )
    )

    # 4. Support Tickets
    replicate_support_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=migrate_collection(
            'SupportTickets',
            'support_tickets',
            lambda doc: {
                'ticket_id': str(doc['ticket_id']),
                'user_id': str(doc['user_id']),
                'status': str(doc['status']),
                'issue_type': str(doc['issue_type']),
                'messages': json.dumps(doc['messages']),
                'created_at': doc['created_at'].isoformat(),
                'updated_at': doc['updated_at'].isoformat()
            }
        )
    )

    # 5. User Recommendations
    replicate_recommendations = PythonOperator(
        task_id='replicate_recommendations',
        python_callable=migrate_collection(
            'UserRecommendations',
            'user_recommendations',
            lambda doc: {
                'user_id': str(doc['user_id']),
                'recommended_products': json.dumps(doc['recommended_products']),
                'last_updated': doc['last_updated'].isoformat()
            }
        )
    )

    # 6. Moderation Queue
    replicate_moderation = PythonOperator(
        task_id='replicate_moderation',
        python_callable=migrate_collection(
            'ModerationQueue',
            'moderation_queue',
            lambda doc: {
                'review_id': str(doc['review_id']),
                'user_id': str(doc['user_id']),
                'product_id': str(doc['product_id']),
                'review_text': str(doc['review_text']),
                'rating': int(doc['rating']),
                'moderation_status': str(doc['moderation_status']),
                'flags': json.dumps(doc['flags']),
                'submitted_at': doc['submitted_at'].isoformat()
            }
        )
    )

    # 7. Search Queries
    replicate_search_queries = PythonOperator(
        task_id='replicate_search_queries',
        python_callable=migrate_collection(
            'SearchQueries',
            'search_queries',
            lambda doc: {
                'query_id': str(doc['query_id']),
                'user_id': str(doc['user_id']),
                'query_text': str(doc['query_text']),
                'timestamp': doc['timestamp'].isoformat(),
                'filters': json.dumps(doc['filters'], default=json_serializer),
                'results_count': int(doc['results_count'])
            }
        )
    )

    # 8. Create Views
    create_views = PythonOperator(
        task_id='create_analytical_views',
        python_callable=create_analytical_views
    )

    # Define dependencies
    (
            replicate_user_sessions
            >> replicate_product_prices
            >> replicate_event_logs
            >> replicate_support_tickets
            >> replicate_recommendations
            >> replicate_moderation
            >> replicate_search_queries
            >> create_views
    )
