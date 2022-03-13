#!/usr/bin/env bash

set -eu

print_message () {
  printf "\033[0;32;1m${1}\033[0m\n"
}

print_message "\xF0\x9F\x95\x90\t Waiting for Airflow Web server to be ready..."
until $(curl --output /dev/null --silent http://airflow-web:8080/health); do
    sleep 1
done

print_message "Airflow Web server is good to go!"
