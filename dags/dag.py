"""
Airflow DAG for audio ingestion from Exotel webhooks
"""
import logging
import tempfile
from datetime import datetime, timedelta
import os
from typing import Any, Dict, Union
from dotenv import load_dotenv, find_dotenv
from pydantic import Field, BaseModel, ValidationError

import requests
from azure.storage.blob import BlobServiceClient
from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from airflow.exceptions import AirflowException
from pendulum import datetime as pendulum_datetime
import psycopg2
from airflow.sdk import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
import clickhouse_connect
from airflow.hooks.base import BaseHook


from sarvam_messaging import get_messaging_broker 


_ = load_dotenv(find_dotenv())


class AudioIngestionParams(BaseModel):
    call_sid: str
    recording_url: str
    recording_available_by: str
    duration: int
    from_number: str = Field(alias='from')
    to_number: str = Field(alias='to')

def upload_audio_to_azure_blob(
    connection_string: str,
    container_name: str,
    blob_name: str,
    local_file_path: str
) -> None:
    """
    Upload audio file to Azure Blob Storage
    
    Args:
        connection_string: Azure storage connection string
        container_name: Target container name
        blob_name: Target blob name/path
        local_file_path: Path to local audio file
        
    Raises:
        Exception: On upload failure
    """
    
    if not os.path.exists(local_file_path):
        raise AirflowException(f"Local file not found: {local_file_path}")
    
    if not os.path.getsize(local_file_path) > 0:
        raise AirflowException(f"File is empty: {local_file_path}")
    
    try:
        blob_client = BlobServiceClient.from_connection_string(
            connection_string,
            connection_timeout=20,
            read_timeout=300 
        ).get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Stream upload to minimize memory usage
        with open(local_file_path, 'rb') as audio_file:
            blob_client.upload_blob(
                audio_file,
                overwrite=True,
                timeout=2  
            )
        
        file_size = os.path.getsize(local_file_path)
        logging.info(f"Uploaded {os.path.basename(local_file_path)} ({file_size} bytes) to {blob_name}")
        
    except Exception as e:
        error_msg = f"Unexpected error uploading {local_file_path}: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


@dag(
    dag_id="audio_ingestion_dag",
    start_date=pendulum_datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "call_sid": "",
        "recording_url": "",
        "recording_available_by": "",
        "duration": 0,
        "from": "",
        "to": "",
    },
    tags=["audio", "exotel"],
)
def audio_ingestion_dag():
    """
    Audio ingestion DAG that orchestrates the complete workflow
    """

    @task
    def validate_params() -> Dict[str, Union[str, Any]]:
        """Validate all inputs before processing"""
        try:
            params = AudioIngestionParams(**get_current_context()['params'])
            return params.dict()
        except ValidationError as e:
            raise AirflowException(f"Invalid parameters: {e}")

    @task
    def calculate_time(params: dict) -> str:
        """Calculate target datetime when audio will be available"""
        recording_available_by = params.get('recording_available_by')
        
        if recording_available_by:
            try:
                if isinstance(recording_available_by, str):
                    if recording_available_by.endswith('Z'):
                        recording_available_by = recording_available_by[:-1] + '+00:00'
                    target_time = datetime.fromisoformat(recording_available_by)
                else:
                    target_time = recording_available_by
                target_time = target_time + timedelta(seconds=15)
                logging.info(f"[DAG] Using provided recording_available_by: {target_time}")
            except Exception as e:
                logging.warning(f"[DAG] Failed to parse recording_available_by: {e}. Using fallback.")
                target_time = datetime.utcnow() + timedelta(minutes=5, seconds=00)
        else:
            target_time = datetime.utcnow() + timedelta(minutes=5, seconds=00)
            logging.info(f"[DAG] No recording_available_by provided, using fallback: {target_time}")

        logging.info(f"[DAG] Audio will be available at: {target_time}")
        return target_time.isoformat()


    @task(retries=3)
    def download(params: dict) -> str:
        """Download audio from recording_url with exponential backoff retry logic"""

        recording_url = params.get('recording_url')
        call_sid = params.get('call_sid')

        username = Variable.get('EXOTEL_SID')
        password = Variable.get('EXOTEL_TOKEN')
        
        if not recording_url or not call_sid:
            raise AirflowException("recording_url and call_sid are required")

        temp_file_path = f"/tmp/{call_sid}.wav"
        
        try:
            logging.info(f"[DAG] Downloading audio from {recording_url}")
            
            response = requests.get(recording_url, auth=(username, password), timeout=60)
            response.raise_for_status()
        
            if len(response.content) == 0:
                raise Exception("Downloaded file is empty")
        
            temp_dir = tempfile.mkdtemp()
            temp_file_path = os.path.join(temp_dir, f"{call_sid}.wav")
        
            with open(temp_file_path, 'wb') as f:
                f.write(response.content)
        
            file_size = os.path.getsize(temp_file_path)
            logging.info(f"[DAG] Successfully downloaded audio to {temp_file_path} (size: {file_size} bytes)")
            return temp_file_path
        
        except Exception as e:
            logging.error(f"[DAG] Download failed: {str(e)}")
            raise AirflowException(f"Failed to download audio: {str(e)}")

    
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def lookup_from_facts_table(params: dict) -> Dict[str, str]:
        """Lookup path parameters from ClickHouse EngagementFacts table"""
        logging.info("Starting lookup_from_facts_table task")
            
        call_sid = params.get('call_sid', '').strip()
        
        if not call_sid:
            logging.warning("call_sid is empty, returning None")
            return "call_sid is empty, No records fetched"
        
        logging.info(f"call_sid found: {call_sid}")

        # Retrieve ClickHouse connection details from Airflow
        try:
            logging.info("Retrieving ClickHouse connection details from Airflow")
            clickhouse_conn = BaseHook.get_connection('clickhouse-qa')
            clickhouse_config = {
                'host': clickhouse_conn.host,
                'port': clickhouse_conn.port,
                'username': clickhouse_conn.login,
                'password': clickhouse_conn.password,
                'database': clickhouse_conn.schema
            }
            logging.info(f"Successfully retrieved ClickHouse connection details for host: {clickhouse_config['host']}")
        except Exception as e:
            logging.error(f"Failed to retrieve ClickHouse connection details: {e}")
            raise AirflowException(f"Could not retrieve ClickHouse connection: {e}")
        
        path_params_query = """
        SELECT org_id, workspace_id, app_id, interaction_id
        FROM EngagementFacts
        WHERE provider_identifier = %(call_sid)s AND is_deleted = 0
        LIMIT 1
        """
        
        client = None
        try:
            logging.info(f"Attempting ClickHouse connection to {clickhouse_config['host']}:{clickhouse_config['port']}")
            
            client = clickhouse_connect.get_client(
                host=clickhouse_config['host'],
                port=int(clickhouse_config['port']),
                username=clickhouse_config['username'],
                password=clickhouse_config['password'],
                database=clickhouse_config['database'],
                secure=False,
                connect_timeout=10,
                send_receive_timeout=30,
            )
            
            logging.info(f"ClickHouse connection established, executing query for call_sid: {call_sid}")
            
            # Execute query with timeout
            result = client.query(
                path_params_query,
                parameters={'call_sid': call_sid}
            )
            logging.info("Query executed successfully")
            
            # Process results
            if result.result_rows:
                path_params = {
                    'org_id': str(result.result_rows[0][0] or 'unknown'),
                    'workspace_id': str(result.result_rows[0][1] or 'unknown'),
                    'app_id': str(result.result_rows[0][2] or 'unknown'),
                    'interaction_id': str(result.result_rows[0][3] or 'unknown'),
                }
                logging.info(f"Successfully retrieved path params from ClickHouse: {path_params}")
                return path_params
            else:
                logging.warning(f"No results found for call_sid: {call_sid} in ClickHouse")
                return {
                    f"No results found for call_sid: {call_sid} in ClickHouse"
                }

                
        except Exception as e:
            logging.error(f"Error fetching path params from ClickHouse: {e}")
            return {
                f"Error fetching path params from ClickHouse: {e}"
            }
            
        finally:
            if client:
                try:
                    client.close()
                    logging.info("ClickHouse connection closed")
                except Exception as e:
                    logging.warning(f"Error closing ClickHouse connection: {e}")

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def upload_audio_to_blob(file_path: str, path_params: dict, params: dict) -> str:
        """Upload the downloaded audio file to azure container"""
        call_sid = params.get('call_sid', 'test_call_sid').strip()

        logging.info(f"[DAG] Retrieved file_path from XCom: {file_path}")
        logging.info(f"[DAG] Retrieved path_params from XCom: {path_params}")

        if not file_path or not os.path.exists(file_path):
            raise AirflowException(f"Audio file not found for upload: {file_path}")

        if not path_params or not isinstance(path_params, dict):
            raise AirflowException(f"Invalid path_params from XCom: {path_params}")

        try:
            # Dynamic blob path based on path_params
            blob_name = f"apps/{path_params['org_id']}/{path_params['workspace_id']}/{path_params['app_id']}/{path_params['interaction_id']}/call_recordings/{call_sid}.wav"
            
            # Get config from environment
            connection_str = Variable.get("AZURE_BLOB_CONNECTION_STRING")
            container_name =  Variable.get("AZURE_BLOB_CONTAINER")

            if not connection_str:
                raise AirflowException("AZURE_BLOB_CONNECTION_STRING is not set in environment")

            logging.info(f"[DAG] Starting upload to blob path: {blob_name}")
            
            upload_audio_to_azure_blob(connection_str, container_name, blob_name, file_path)

            # Build blob URL
            account_url = BlobServiceClient.from_connection_string(connection_str).url
            blob_url = f"{account_url}/{container_name}/{blob_name}"

            logging.info(f"[DAG] Uploaded to Azure Blob Storage: {blob_url}")

            # Cleanup
            try:
                os.remove(file_path)
                parent_dir = os.path.dirname(file_path)
                if not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                logging.info(f"[DAG] Cleaned up temporary file: {file_path}")
            except Exception as cleanup_err:
                logging.warning(f"[DAG] Cleanup failed: {cleanup_err}")

            return blob_url

        except Exception as e:
            logging.error(f"[DAG] Upload to Azure Blob Storage failed: {e}")
            raise AirflowException(f"Blob upload failed: {e}")

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def publish_to_audio_ingestion_topic(blob_url: str, path_params: dict, params: dict):
        """Fixed version using asyncio.run()"""
        import asyncio
        
        async def async_publish():
            """Async inner function containing all async operations"""
            if not blob_url:
                raise AirflowException("blob_url not found in XCom from upload_audio_to_blob task")
            if not path_params:
                raise AirflowException("path_params not found in XCom from lookup_from_facts_table task")
            
            # Get and validate environment variables
            required_env_vars = {
                'eventhub_connection_string': os.environ.get("EVENTHUB_CONNECTION_STRING"),
                'storage_connection_string': os.environ.get("AZURE_BLOB_CONNECTION_STRING"),
                'topic_name': os.environ.get("topic_name")
            }
            
            missing_vars = [key for key, value in required_env_vars.items() if not value]
            if missing_vars:
                raise AirflowException(f"Missing required environment variables: {', '.join(missing_vars)}")
            
            # Create event payload
            audio_event = {
                "call_sid": params.get('call_sid'),
                "blob_url": blob_url,
                "path_params": path_params,
                "from_number": params.get('from'),
                "to_number": params.get('to'),
                "duration": params.get('duration'),
                "recording_url": params.get('recording_url'),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            logging.info(f"Publishing event for call_sid: {params.get('call_sid')} to topic: {required_env_vars['topic_name']}")
            
            try:            
                # Create broker and send event
                broker = get_messaging_broker(
                    connection_string=required_env_vars['eventhub_connection_string'],
                )
                
                producer = broker.get_producer(topic=required_env_vars['topic_name'])

                result = await producer.send(audio_event)
                
                logging.info(f"Successfully published message to Event Hub: {result}")
                return {"topic": required_env_vars['topic_name'], "result": str(result)}
                
            except ImportError as e:
                raise AirflowException(f"sarvam_messaging import failed: {e}")
            except (ConnectionError, TimeoutError) as e:
                raise AirflowException(f"Event Hub connection failed: {e}")
            except Exception as e:
                logging.error(f"Event Hub publish failed: {type(e).__name__}: {e}")
                raise AirflowException(f"Event Hub publish failed: {e}")
        
        return asyncio.run(async_publish())

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def update_to_audiojobtracker(params: dict) -> None:
        
        # Validate required parameters
        call_sid = params.get('call_sid')

        if not call_sid:
            raise AirflowException("call_sid is required in conf")

        #initializing here --> will be overwritten in try block
        connection = None
        cursor = None

        try:

            logging.info("Retrieving PostgreSQL connection details from Airflow")
            # must be configured on airflow web ui
            postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
            connection = postgres_hook.get_conn()
            connection.autocommit = False
            
            # Test connection with a simple query
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 as test, version() as pg_version, NOW() as current_time")
                test_result = cursor.fetchone()
                logging.info(f"Connection test successful. PostgreSQL version: {test_result[1]}")
                logging.info(f"Database server time: {test_result[2]}")
                
            except Exception as test_error:
                raise AirflowException(f"Connection test failed: {test_error}")
        
            expiry_time = datetime.utcnow() + timedelta(hours=24)
            updated_at = datetime.utcnow()
            #TODO: populate duration field as well
            update_query = """
                UPDATE audio_job_tracker
                SET 
                    expiry_time = %s,
                    updated_at = %s
                WHERE call_sid = %s
                RETURNING id, expiry_time
                """
            
            # Prepare parameters
            parameters = [expiry_time, updated_at , call_sid]
            
            try:
                logging.info("Executing update query...")
                cursor.execute(update_query, parameters)
                
                # Fetch the result
                updated_record = cursor.fetchone()
                
                if not updated_record:
                    raise AirflowException(f"Update query executed but no rows were affected for call_sid: {call_sid}")
                
                # Commit the transaction
                connection.commit()
                logging.info("Transaction committed successfully")
            except Exception as update_error:
                connection.rollback()
                logging.error(f"Update query failed: {update_error}")
                raise AirflowException(f"Failed to update record for call_sid {call_sid}: {update_error}")
            
            logging.info(f"AudioJobTracker updated successfully - call_sid: {call_sid}")
            
            return None
            
        except Exception as e:
            logging.error(f"Failed to update AudioJobTracker: {str(e)}")
            raise AirflowException(f"AudioJobTracker update failed for call_sid {call_sid}: {str(e)}")

        finally:
            # Clean up database resources
            try:
                if cursor:
                    cursor.close()
                    logging.info("Database cursor closed")
                if connection:
                    connection.close()
                    logging.info("Database connection closed")
            except Exception as cleanup_error:
                logging.warning(f"Error during cleanup: {cleanup_error}")

    # Task flow definition
    params_task = validate_params()
    target_time = calculate_time(params=params_task)
    audio_file = download(params=params_task)
    lookup = lookup_from_facts_table(params=params_task)
    update_tracker_table = update_to_audiojobtracker(params=params_task)

    delay_sensor = DateTimeSensorAsync(
        task_id='delay',
        target_time="{{ ti.xcom_pull(task_ids='calculate_time') }}",
        timeout=500,
        poke_interval=10,
        mode='reschedule'
    )

    blob_url = upload_audio_to_blob(
        file_path=audio_file,
        path_params=lookup,
        params=params_task
    )

    publish_audio = publish_to_audio_ingestion_topic(
        blob_url=blob_url,
        path_params=lookup,
        params=params_task
    )

    # Task dependencies
    params_task >> [target_time, lookup]
    target_time >> delay_sensor >> audio_file
    [audio_file, lookup] >> blob_url >> publish_audio >> update_tracker_table
    


dag_instance = audio_ingestion_dag()