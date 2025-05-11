#!/bin/bash
set -e

# ==============================================================================
# Starts development server using Docker Compose.
# ------------------------------------------------------------------------------
BLUE='\033[1;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}===========================================================${NC}"
echo -e "${BLUE}🐳 Starting compose -f compose-dev.yaml up airflow-init ${NC}"
echo -e "${BLUE}===========================================================${NC}"
docker compose -f compose-dev.yaml up airflow-init

echo -e "${BLUE}===========================================================${NC}"
echo -e "${BLUE}🐳 Starting docker compose -f compose-dev.yaml up ${NC}"
echo -e "${BLUE}===========================================================${NC}"
docker compose -f compose-dev.yaml up postgres redis keycloak airflow-webserver airflow-scheduler airflow-worker localstack
