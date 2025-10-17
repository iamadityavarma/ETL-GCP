#!/usr/bin/env bash
set -euo pipefail

############################
# CONFIG (edit these)
############################
PROJECT_ID="${PROJECT_ID:-fluent-grin-474614-d8}"
REGION="${REGION:-us-central1}"
NETWORK="${NETWORK:-default}"                 # VPC name
SUBNET="${SUBNET:-default}"                  # Subnet name in REGION
SQL_INSTANCE="${SQL_INSTANCE:-pg-staging}"   # Cloud SQL instance name
CONNECTOR="${CONNECTOR:-serverless-us-central1}"
COMPOSER_SA="${COMPOSER_SA:-etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com}"
BQ_DATASET="${BQ_DATASET:-staging}"
REQUIRED_APIS=(
  composer.googleapis.com
  storage.googleapis.com
  iam.googleapis.com
  compute.googleapis.com
  vpcaccess.googleapis.com
  run.googleapis.com
  sqladmin.googleapis.com
  servicenetworking.googleapis.com
  secretmanager.googleapis.com
  bigquery.googleapis.com
)
SECRETS_EXPECTED=(PG_HOST PG_PORT PG_DB PG_USER PG_PASSWORD API_KEY BQ_PROJECT BQ_DATASET BQ_TABLE)

############################
# Helpers
############################
ok()    { echo -e "✅ $*"; }
warn()  { echo -e "⚠️  $*"; }
fail()  { echo -e "❌ $*"; }

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "Missing CLI: $1"; exit 1
  fi
}

gval() { gcloud --quiet "$@" 2>/dev/null; }

iam_has_role() {
  local member="$1" role="$2"
  gcloud projects get-iam-policy "$PROJECT_ID" \
    --flatten="bindings[].members" \
    --format="value(bindings.role)" \
    --filter="bindings.members:$member" | grep -Fxq "$role"
}

############################
# Checks
############################
echo "=== Cloud Composer Preflight (PROJECT=$PROJECT_ID, REGION=$REGION) ==="

need gcloud
need bq

# 1) Project
CUR_PROJECT=$(gcloud config get-value core/project 2>/dev/null || true)
if [[ "$CUR_PROJECT" != "$PROJECT_ID" ]]; then
  warn "gcloud project is '$CUR_PROJECT', expected '$PROJECT_ID'. Run: gcloud config set project $PROJECT_ID"
else
  ok "gcloud project set to $PROJECT_ID"
fi

# 2) Billing
if gval beta billing projects describe "$PROJECT_ID" --format="value(billingEnabled)" | grep -qi true; then
  ok "Billing is enabled"
else
  fail "Billing is NOT enabled for $PROJECT_ID"; exit 1
fi

# 3) APIs
MISSING_APIS=()
for api in "${REQUIRED_APIS[@]}"; do
  if ! gval services list --enabled --filter="name:$api" --format="value(name)" | grep -q "$api"; then
    MISSING_APIS+=("$api")
  fi
done
if ((${#MISSING_APIS[@]})); then
  fail "Missing APIs: ${MISSING_APIS[*]}"
  echo "   Enable with: gcloud services enable ${MISSING_APIS[*]}"
  exit 1
else
  ok "All required APIs are enabled"
fi

# 4) VPC & Subnet
if gval compute networks describe "$NETWORK" >/dev/null; then
  ok "VPC '$NETWORK' exists"
else
  fail "VPC '$NETWORK' not found"; exit 1
fi

SUBNET_OK=$(gval compute networks subnets list --filter="name=$SUBNET AND region:($REGION)" --format="value(name)")
if [[ -n "$SUBNET_OK" ]]; then
  ok "Subnet '$SUBNET' exists in $REGION"
else
  fail "Subnet '$SUBNET' not found in $REGION"; exit 1
fi

# 5) Private Service Range & VPC Peering (for Cloud SQL Private IP)
PSR_NAME="google-managed-services-$NETWORK"
PSR_OK=$(gval compute addresses list --global --filter="name=$PSR_NAME" --format="value(name)")
if [[ -n "$PSR_OK" ]]; then
  ok "Private service range '$PSR_NAME' exists"
else
  fail "Private service range '$PSR_NAME' missing."
  echo "   Create with:"
  echo "   gcloud compute addresses create $PSR_NAME --global --purpose=VPC_PEERING --addresses=10.50.0.0 --prefix-length=24 --network=$NETWORK"
  exit 1
fi

PEERING_OK=$(gval services vpc-peerings list --network="$NETWORK" --format="value(name)" | grep -F "servicenetworking" || true)
if [[ -n "$PEERING_OK" ]]; then
  ok "Service Networking peering is configured"
else
  fail "Service Networking peering missing."
  echo "   Create with:"
  echo "   gcloud services vpc-peerings connect --service=servicenetworking.googleapis.com --network=$NETWORK --ranges=$PSR_NAME"
  exit 1
fi

# 6) Serverless VPC Connector
if gval compute networks vpc-access connectors describe "$CONNECTOR" --region="$REGION" >/dev/null; then
  ok "Serverless VPC Connector '$CONNECTOR' exists in $REGION"
else
  fail "Serverless VPC Connector '$CONNECTOR' missing."
  echo "   Create with:"
  echo "   gcloud compute networks vpc-access connectors create $CONNECTOR --region=$REGION --network=$NETWORK --range=10.8.0.0/28"
  exit 1
fi

# 7) Cloud NAT (optional but recommended if outbound internet required)
if gval compute routers nats list --region="$REGION" --format="value(name)" | grep -q .; then
  ok "Cloud NAT exists in $REGION"
else
  warn "Cloud NAT not found in $REGION (needed for outbound internet from private workloads)."
  echo "   Create with:"
  echo "   gcloud compute routers create nat-router --region=$REGION --network=$NETWORK"
  echo "   gcloud compute routers nats create nat-config --router=nat-router --router-region=$REGION --nat-all-subnet-ip-ranges --auto-allocate-nat-external-ips"
fi

# 8) Cloud SQL Instance & Private IP
if gval sql instances describe "$SQL_INSTANCE" --format="value(name)" | grep -q "$SQL_INSTANCE"; then
  ok "Cloud SQL instance '$SQL_INSTANCE' exists"
else
  fail "Cloud SQL instance '$SQL_INSTANCE' not found"; exit 1
fi

if gval sql instances describe "$SQL_INSTANCE" --format="value(ipAddresses[0].type)" | grep -qi PRIVATE; then
  PRIV_IP=$(gval sql instances describe "$SQL_INSTANCE" --format="value(ipAddresses[0].ipAddress)")
  ok "Cloud SQL has Private IP: $PRIV_IP"
else
  fail "Cloud SQL instance '$SQL_INSTANCE' does NOT have Private IP enabled."
  echo "   Patch with:"
  echo "   gcloud sql instances patch $SQL_INSTANCE --network=projects/$PROJECT_ID/global/networks/$NETWORK --no-assign-ip"
  exit 1
fi

# 9) Secret Manager: required secrets present
MISSING_SECRETS=()
for s in "${SECRETS_EXPECTED[@]}"; do
  if ! gval secrets describe "$s" >/dev/null; then
    MISSING_SECRETS+=("$s")
  fi
done
if ((${#MISSING_SECRETS[@]})); then
  fail "Missing secrets: ${MISSING_SECRETS[*]}"
  echo "   Create secrets, e.g.: printf 'value' | gcloud secrets create NAME --data-file=-"
  # not fatal if you want to continue; uncomment to enforce:
  # exit 1
else
  ok "All expected secrets exist"
fi

# 10) BigQuery dataset
if bq --location="$REGION" ls -d | awk '{print $1}' | grep -Fxq "$PROJECT_ID:$BQ_DATASET"; then
  ok "BigQuery dataset '$PROJECT_ID:$BQ_DATASET' exists"
else
  fail "BigQuery dataset '$PROJECT_ID:$BQ_DATASET' not found"
  echo "   Create with: bq --location=$REGION mk -d $PROJECT_ID:$BQ_DATASET"
  # exit 1
fi

# 11) Composer Service Account exists
if gval iam service-accounts list --format="value(email)" | grep -Fxq "$COMPOSER_SA"; then
  ok "Composer Service Account exists: $COMPOSER_SA"
else
  fail "Composer Service Account missing: $COMPOSER_SA"
  echo "   Create with: gcloud iam service-accounts create $(echo "$COMPOSER_SA" | cut -d@ -f1) --display-name='ETL Composer SA'"
  exit 1
fi

# 12) Composer SA roles
declare -a ROLES_REQ=(
  roles/composer.worker
  roles/run.admin
  roles/iam.serviceAccountUser
  roles/secretmanager.secretAccessor
  roles/bigquery.dataEditor
  roles/cloudsql.client
)
MISSING_ROLES=()
for r in "${ROLES_REQ[@]}"; do
  if ! iam_has_role "serviceAccount:$COMPOSER_SA" "$r"; then
    MISSING_ROLES+=("$r")
  fi
done
if ((${#MISSING_ROLES[@]})); then
  fail "Composer SA missing roles: ${MISSING_ROLES[*]}"
  echo "   Grant with:"
  for r in "${MISSING_ROLES[@]}"; do
    echo "   gcloud projects add-iam-policy-binding $PROJECT_ID --member='serviceAccount:$COMPOSER_SA' --role='$r'"
  done
  # exit 1
else
  ok "Composer SA has required roles"
fi

# 13) gcloud version
if gcloud version | grep -qi 'Google Cloud SDK'; then
  ok "gcloud present: $(gcloud version | head -n1)"
else
  warn "Cannot parse gcloud version"
fi

echo "=== Preflight complete ==="
echo "If all required items are ✅, you can create Composer:"
echo "gcloud composer environments create etl-dag --location=$REGION --service-account=$COMPOSER_SA"
