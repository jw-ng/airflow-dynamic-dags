FROM mongo

WORKDIR /tmp

COPY ./seeder.json seeder.json

CMD mongoimport --host airflow-mongodb --db local --collection configs --type json --file seeder.json --jsonArray
