if [ ! -z "$INIT" ]
then
  echo "Initialisation airflow"
  docker compose up airflow-init
fi
docker compose up -d
cd superset
docker compose  -f ./docker-compose-image-tag.yml up -d
