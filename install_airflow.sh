helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow -f values.yaml