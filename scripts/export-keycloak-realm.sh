#!/bin/bash
# ==============================================================================
# Exports the Keycloak realm configuration for the bluecore realm
# to the path "keycloak-export/bluecore-realm.json" using docker compose.
# ------------------------------------------------------------------------------
GREEN='\033[0;32m'
BLUE='\033[1;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}===========================================================${NC}"
echo -e "${BLUE}🔄 Starting export of 'bluecore' Keycloak realm...${NC}"
echo -e "${BLUE}Target path: keycloak-export/bluecore-realm.json${NC}"
echo -e "${BLUE}===========================================================${NC}"

docker compose -f compose-dev.yaml run --rm keycloak \
  export --dir=/opt/keycloak/data/export --realm=bluecore --users=realm_file

if [ $? -eq 0 ]; then
  echo -e "${GREEN}===========================================================${NC}"
  echo -e "${GREEN}✅ Export completed successfully.${NC}"
  echo -e "${GREEN}===========================================================${NC}"
else
  echo -e "${RED}===========================================================${NC}"
  echo -e "${RED}❌ Export failed. Check logs above for details.${NC}"
  echo -e "${RED}===========================================================${NC}"
fi
