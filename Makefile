clean:
	source .envrc && \
	docker-compose -p airflow-dynamic-dags \
	-f ./setup/docker-compose.yml \
	-f ./setup/docker-compose-dev.yml \
	-f ./setup/docker-compose-tests.yml \
	down --remove-orphans --volumes

dev: clean
	source .envrc && \
	docker-compose -p airflow-dynamic-dags \
	-f ./setup/docker-compose.yml \
	-f ./setup/docker-compose-dev.yml \
	up -d

wait_for_airflow_web_to_be_healthy:
	until [ $$(docker inspect -f '{{.State.Health.Status}}' airflow-web) = "healthy" ] ; do \
		sleep 1 ; \
	done

seeded_dev: dev wait_for_airflow_web_to_be_healthy
	docker exec airflow-scheduler sh -c \
	"airflow connections import /tmp/seed/connections.yaml && airflow variables import /tmp/seed/variables.json"

unit_test:
	PYTHONPATH="dags/" python3 -m pytest -vvv -s --ignore ./venv
