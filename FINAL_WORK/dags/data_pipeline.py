from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from psycopg2.extras import execute_batch
import psycopg2
import json
import logging
from typing import Any, List

logger = logging.getLogger(__name__)


def json_serializer(obj: Any) -> str:
    """Custom JSON serializer for datetime objects"""
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


def migrate_collection(
        collection_name: str,
        table_name: str,
        transform_fn: callable,
        key_fields: List[str],
        batch_size: int = 1000
):
    def _migrate():
        try:
            logger.info(f"Starting incremental sync for {collection_name}")

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

            # Get last sync timestamp
            cur.execute(f"SELECT MAX(last_modified) FROM {table_name}")
            last_ts = cur.fetchone()[0] or datetime.min

            # MongoDB query with projection
            query = {"last_modified": {"$gt": last_ts}}
            projection = {"_id": 0}
            cursor = mongo_col.find(query, projection).sort("last_modified", 1)

            # Batch processing setup
            columns, data_batch = [], []
            insert_query = None

            for doc in cursor:
                try:
                    data = transform_fn(doc)

                    if not columns:
                        columns = list(data.keys())
                        placeholders = ['%s'] * len(columns)
                        update_fields = [f for f in columns if f not in key_fields]
                        update_set = ', '.join([f"{f} = EXCLUDED.{f}" for f in update_fields])

                        insert_query = f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT ({', '.join(key_fields)})
                            DO UPDATE SET {update_set}
                        """

                    data_batch.append(tuple(data.values()))

                    # Execute batch insert
                    if len(data_batch) >= batch_size:
                        execute_batch(cur, insert_query, data_batch)
                        conn.commit()
                        logger.info(f"Inserted {len(data_batch)} records")
                        data_batch = []

                except Exception as e:
                    logger.error(f"Error in document: {str(e)}")
                    continue

            # Insert remaining records
            if data_batch:
                execute_batch(cur, insert_query, data_batch)
                conn.commit()
                logger.info(f"Inserted final {len(data_batch)} records")

            logger.info(f"Total processed: {cursor.retrieved} records")

        except Exception as e:
            logger.error(f"Critical error: {str(e)}", exc_info=True)
            raise
        finally:
            conn.close()
            mongo_client.close()

    return _migrate


def refresh_analytical_views():
    """Refresh materialized views in PostgreSQL"""
    conn = psycopg2.connect(
        dbname="analytics",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    try:
        cur.execute("REFRESH MATERIALIZED VIEW user_activity")
        cur.execute("REFRESH MATERIALIZED VIEW support_efficiency")
        conn.commit()
        logger.info("Successfully refreshed materialized views")
    except Exception as e:
        logger.error(f"Error refreshing views: {str(e)}")
        raise
    finally:
        conn.close()


with DAG(
        'data_replication',
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule_interval='@hourly',
        catchup=False,
        tags=['data_pipeline'],
        max_active_runs=1
) as dag:
    # Region: Collection replication tasks
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
                'device': doc['device'],
                'actions': json.dumps(doc['actions']),
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['session_id'],
            batch_size=2000
        )
    )

    replicate_product_prices = PythonOperator(
        task_id='replicate_product_prices',
        python_callable=migrate_collection(
            'ProductPriceHistory',
            'product_price_history',
            lambda doc: {
                'product_id': str(doc['product_id']),
                'price_changes': json.dumps(
                    [{
                        'date': change['date'].isoformat(),
                        'price': float(change['price'])
                    } for change in doc['price_changes']],
                    default=json_serializer
                ),
                'current_price': float(doc['current_price']),
                'currency': doc['currency'],
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['product_id'],
            batch_size=1000
        )
    )

    replicate_event_logs = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=migrate_collection(
            'EventLogs',
            'event_logs',
            lambda doc: {
                'event_id': str(doc['event_id']),
                'timestamp': doc['timestamp'].isoformat(),
                'event_type': doc['event_type'],
                'details': json.dumps(doc['details'], default=json_serializer),
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['event_id']
        )
    )

    replicate_support_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=migrate_collection(
            'SupportTickets',
            'support_tickets',
            lambda doc: {
                'ticket_id': str(doc['ticket_id']),
                'user_id': str(doc['user_id']),
                'status': doc['status'],
                'issue_type': doc['issue_type'],
                'messages': json.dumps(doc['messages']),
                'created_at': doc['created_at'].isoformat(),
                'updated_at': doc['updated_at'].isoformat(),
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['ticket_id']
        )
    )

    replicate_recommendations = PythonOperator(
        task_id='replicate_recommendations',
        python_callable=migrate_collection(
            'UserRecommendations',
            'user_recommendations',
            lambda doc: {
                'user_id': str(doc['user_id']),
                'recommended_products': json.dumps(doc['recommended_products']),
                'last_updated': doc['last_updated'].isoformat(),
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['user_id']
        )
    )

    replicate_moderation = PythonOperator(
        task_id='replicate_moderation',
        python_callable=migrate_collection(
            'ModerationQueue',
            'moderation_queue',
            lambda doc: {
                'review_id': str(doc['review_id']),
                'user_id': str(doc['user_id']),
                'product_id': str(doc['product_id']),
                'review_text': doc['review_text'],
                'rating': doc['rating'],
                'moderation_status': doc['moderation_status'],
                'flags': json.dumps(doc['flags']),
                'submitted_at': doc['submitted_at'].isoformat(),
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['review_id']
        )
    )

    replicate_search_queries = PythonOperator(
        task_id='replicate_search_queries',
        python_callable=migrate_collection(
            'SearchQueries',
            'search_queries',
            lambda doc: {
                'query_id': str(doc['query_id']),
                'user_id': str(doc['user_id']),
                'query_text': doc['query_text'],
                'timestamp': doc['timestamp'].isoformat(),
                'filters': json.dumps(doc['filters'], default=json_serializer),
                'results_count': doc['results_count'],
                'last_modified': doc['last_modified'].isoformat()
            },
            key_fields=['query_id']
        )
    )

    # Region: Post-processing tasks
    refresh_views = PythonOperator(
        task_id='refresh_analytical_views',
        python_callable=refresh_analytical_views
    )

    # Define workflow
    (
            replicate_user_sessions
            >> replicate_product_prices
            >> replicate_event_logs
            >> replicate_support_tickets
            >> replicate_recommendations
            >> replicate_moderation
            >> replicate_search_queries
            >> refresh_views
    )
