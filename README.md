# Airflow Dynamic DAGs

This repository serves as a test bed for the various methods to implement dynamic DAGs
in Airflow.

## Setup

As this project primarily uses `pipenv`, run the following to install the necessary
packages:

```shell
pipenv install
```

Alternatively, you may use `pip` to install the necessary packages:

```shell
pip install -r requirements.txt
```

## Testing

### Installing test requirements

You will first need to install the dev requirements:

```shell
pipenv install --dev
```

Alternatively, you may use `pip` to install the dev packages:

```shell
pip install -r requirements-dev.txt
```

### Running the tests

Use the following `make` command to run the unit tests:

```shell
make unit_tests
```

## Spinning up Airflow locally

### Setting up environment variables

You will first need to create the `.envrc` file using the `.envrc.template` file:

```shell
cp .envrc.template .envrc
```

Make the necessary modifications to the values in the `.envrc` file, as the sensitive
values are just placeholders in the template file.

### Starting the services

You can also spin up an Airflow webserver and the other dependent services required by
the DAGs in this repository using the following `make` command:

```shell
make dev
```

To spin up the services with seeded Airflow connections and variables, and also seeded
MongoDB collections, use:

```shell
make seeded_dev
```
