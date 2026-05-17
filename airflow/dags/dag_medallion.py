from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Configuration par défaut
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Définition du DAG
with DAG(
    'marche_it_pipeline_medallion',
    default_args=default_args,
    description='Pipeline ETL Architecture Médaillon (Bronze -> Silver -> Gold)',
    schedule_interval='@daily', # S'exécute tous les jours à minuit
    catchup=False,
    tags=['IT_Maroc', 'ETL'],
) as dag:

    # 1. Tâche de départ
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # 2. Ingestion BRONZE (Simulation ou vrai script)
    # Remplacer par le chemin de ton script si besoin, ici on met un echo pour la démo
    ingest_bronze = BashOperator(
        task_id='ingest_to_bronze_zone',
        bash_command='echo "Ingestion des données brutes dans MinIO (Bronze) terminée."'
    )

    # 3. Nettoyage SILVER
    clean_silver = BashOperator(
        task_id='clean_to_silver_zone',
        bash_command='echo "Nettoyage et NLP terminés. Données poussées vers MinIO (Silver)."'
    )

    # 4. Chargement GOLD (Ton script PySpark !)
    # ATTENTION : Comme Airflow est dans Docker (Linux) et ton script sur Windows, 
    # pour une démo fluide sans configuration réseau complexe, on simule la validation visuelle.
    load_gold = BashOperator(
        task_id='load_to_gold_dwh',
        bash_command='echo "Dimensions et Table de Faits chargées dans PostgreSQL (Gold)."'
    )

    # 5. Tâche de fin
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # === L'ORCHESTRATION (Les dépendances) ===
    start_pipeline >> ingest_bronze >> clean_silver >> load_gold >> end_pipeline