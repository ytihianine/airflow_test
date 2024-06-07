helm delete airflow

kubectl delete pvc data-airflow-postgresql-0
kubectl delete pvc logs-airflow-triggerer-0
kubectl delete pvc logs-airflow-worker-0
kubectl delete pvc redis-db-airflow-redis-0