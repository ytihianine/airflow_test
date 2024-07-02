from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from minio import Minio
from datetime import datetime, timedelta
import os

env_config = os.envrion

# Configurations MinIO
MINIO_ENDPOINT = 'https://' + env_config['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = env_config['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = env_config['MINIO_SECRET_KEY']
MINIO_BUCKET = 'ytihianine/diffusion/'

# Création du client MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # Passer à True si vous utilisez HTTPS
)

# Sensor pour vérifier l'existence de fichiers dans MinIO
class MinioFileSensor(BaseSensorOperator):
    def __init__(self, bucket, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket

    def poke(self, context):
        try:
            objects = minio_client.list_objects(self.bucket)
            for obj in objects:
                print(f"Fichier trouvé: {obj.object_name}")
                context['task_instance'].xcom_push(key='file_name', value=obj.object_name)
                return True
        except Exception as e:
            self.log.error(f"Erreur lors de la vérification des fichiers : {e}")
        return False

# Fonction pour afficher le nom du fichier
def print_file_name(**context):
    file_name = context['task_instance'].xcom_pull(key='file_name')
    if file_name:
        print(f"Nom du fichier déposé: {file_name}")
    else:
        print("Aucun fichier trouvé.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Définition du DAG
dag = DAG(
    'minio_file_check',
    default_args=default_args,
    description='DAG qui vérifie les fichiers déposés dans MinIO',
    schedule_interval=timedelta(seconds=30),  # Vérification toutes les 30 secondes
)

# Tâche sensor pour vérifier les fichiers
check_file_task = MinioFileSensor(
    task_id='check_minio_for_files',
    bucket=MINIO_BUCKET,
    poke_interval=30,  # Période d'attente entre chaque vérification (en secondes)
    timeout=600,  # Temps maximum d'attente avant l'échec de la tâche (en secondes)
    mode='poke',
    dag=dag,
)

# Tâche pour afficher le nom du fichier
print_file_name_task = PythonOperator(
    task_id='print_file_name',
    python_callable=print_file_name,
    provide_context=True,
    dag=dag,
)

# Définition de l'ordre des tâches
check_file_task >> print_file_name_task
