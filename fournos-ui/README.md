# Fournos Dashboard

A web dashboard for managing [Fournos](https://github.com/openshift-psap/fournos-operator) performance testing jobs on Kubernetes. Submit jobs, monitor live runs, schedule recurring tests, and review historical results -- all from one place.

## What It Does

- **Live job monitoring** -- Watch running FournosJobs with real-time log streaming (SSE) and pipeline progress tracking.
- **Job submission** -- Submit new FournosJobs with project, preset, cluster, and config override selection. Optionally pick an open Forge PR to test with -- the dashboard fetches open PRs from GitHub and fills in the commit SHA automatically.
- **GitHub PR integration** -- Lists open pull requests from the [Forge repo](https://github.com/openshift-psap/forge) directly in the submit form. Since Forge is a public repository, no GitHub token is needed.
- **Scheduling** -- Create Kubernetes CronJobs for recurring test runs, with optional version-resolver scripts that dynamically determine parameters at runtime.
- **History** -- Browse completed jobs stored in PostgreSQL with status, duration, and direct links to MLflow artifacts.
- **Schedule tracking** -- See which schedule triggered each job (manual vs. scheduled) and view all runs for a given schedule.

## Architecture

```
                         ┌─── Pod ──────────────────────────────┐
┌─────────┐   ┌───────┐ │ ┌─────────────┐   ┌───────────────┐ │   ┌────────────┐
│ Browser │──▶│ Route │──┼▶│ OAuth Proxy │──▶│ FastAPI + HTMX│─┼──▶│ Kubernetes │
│         │◀──│ (TLS) │◀─┼─│  (:8443)    │◀──│   (:8000)     │◀┼───│    API     │
└─────────┘   └───────┘ │ └──────┬──────┘   └───────┬───────┘ │   └────────────┘
                         │        │                   │         │
                         │        ▼                   ▼         │
                         │  OpenShift OAuth    ┌────────────┐   │
                         │    Server           │ PostgreSQL │   │
                         │                     └────────────┘   │
                         └──────────────────────────────────────┘
```

- **FastAPI** backend with **Jinja2** templates and **HTMX** for dynamic updates.
- **Kubernetes Python client** for watching FournosJob CRs, streaming pod logs, and managing CronJobs.
- **PostgreSQL** (via SQLAlchemy async + asyncpg) for persisting job metadata and schedule tracking.
- A background **watcher thread** monitors FournosJob events and archives them to PostgreSQL automatically.

## Prerequisites

- An OpenShift cluster (4.14+) with the [Fournos Operator](https://github.com/openshift-psap/fournos-operator) installed.
- **cert-manager** operator installed on the cluster (for TLS certificate issuance).
- A container registry to push the dashboard image.
- `oc` CLI configured with cluster-admin access (needed for initial setup).

## Getting Started

### 1. Configure the overlay

```bash
cd kustomize/overlays/ocp/
```

Create the required config files from the examples:

```bash
cp kustomization.yaml.example kustomization.yaml
cp oauth-cookie-secret.yaml.example oauth-cookie-secret.yaml
cp projects.yaml.example projects.yaml
cp ../../base/postgresql-secret.env.example postgresql-secret.env
```

These files are gitignored because they contain secrets or cluster-specific values.
For an existing deployment, you can pull values from the cluster instead (see "Pulling config from a live cluster" below).

Edit each file with your values:
- **`projects.yaml`** -- Define your Forge projects, clusters, and presets.
- **`postgresql-secret.env`** -- Set your database credentials (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE).

Edit `kustomization.yaml` and replace these values:
- Dashboard container image (e.g. `quay.io/your-org/fournos-dashboard:latest`)
- `FOURNOS_NAMESPACE` -- the namespace where FournosJobs run
- `storageClassName` -- your cluster's storage class (`oc get sc` to list)

Generate the OAuth cookie secret:

```bash
# Replace the placeholder in oauth-cookie-secret.yaml
COOKIE=$(openssl rand -base64 32)
# macOS:
sed -i '' "s|REPLACE_ME_WITH_OUTPUT_OF_openssl_rand_base64_32|${COOKIE}|" oauth-cookie-secret.yaml
# Linux:
# sed -i "s|REPLACE_ME_WITH_OUTPUT_OF_openssl_rand_base64_32|${COOKIE}|" oauth-cookie-secret.yaml
```

Verify the OAuth proxy image matches your cluster version:

```bash
# Get the correct image for your cluster (digest may differ per OCP version)
oc adm release info --image-for=oauth-proxy
```

If the output differs from what's in `patch-deployment-oauth-proxy.yaml`, update the image field in that file.

### 2. Build and push the dashboard image

```bash
podman build -t quay.io/your-org/fournos-dashboard:latest .
podman push quay.io/your-org/fournos-dashboard:latest
```

### 3. Deploy TLS certificate (one-time)

The dashboard uses a Let's Encrypt certificate for trusted HTTPS. This requires cert-manager to be installed on the cluster.

```bash
# Apply the ClusterIssuer (cluster-scoped, only needed once)
oc apply -f kustomize/overlays/ocp/letsencrypt-clusterissuer.yaml

# Verify it's ready
oc get clusterissuer letsencrypt-production
```

### 4. Deploy to the cluster

```bash
# Apply the main stack
oc apply -k kustomize/overlays/ocp/

# Apply the cross-namespace RoleBinding (grants dashboard access to the jobs namespace)
# Replace FOURNOS_NAMESPACE with your target namespace (e.g. psap-automation)
oc apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: fournos-dashboard
  namespace: FOURNOS_NAMESPACE
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: fournos-dashboard
subjects:
  - kind: ServiceAccount
    name: fournos-dashboard
    namespace: fournos-dashboard
EOF
```

This creates:
- A `fournos-dashboard` namespace
- PostgreSQL StatefulSet with persistent storage
- Dashboard Deployment with OAuth proxy sidecar (2 containers)
- Service, ServiceAccount (with OAuth redirect annotation)
- ClusterRole for FournosJob/CronJob/Pod access
- RoleBinding in the target namespace
- OpenShift Route with reencrypt TLS
- Let's Encrypt Certificate (auto-issued and auto-renewed by cert-manager)
- OAuth cookie secret

### 5. Verify the deployment

```bash
# Pod should show 2/2 Ready (dashboard + oauth-proxy)
oc get pods -n fournos-dashboard

# Check the TLS certificate was issued
oc get certificate fournos-dashboard-cert -n fournos-dashboard

# Get the Route URL
oc get route fournos-dashboard -n fournos-dashboard -o jsonpath='{.spec.host}'
```

### 6. Access the dashboard

Open the Route URL in your browser:

```
https://fournos-dashboard-fournos-dashboard.apps.<cluster-domain>
```

You will be redirected to the OpenShift login page. After authenticating with your cluster credentials, you'll land on the dashboard. Any user who can log into the OpenShift cluster can access the UI.

### Pulling config from a live cluster

If the dashboard is already deployed and you need to recreate the overlay files:

```bash
# projects.yaml
oc get configmap fournos-projects -n fournos-dashboard -o jsonpath='{.data.projects\.yaml}' > projects.yaml

# postgresql-secret.env
oc get secret postgresql-secret -n fournos-dashboard -o go-template='PGHOST={{index .data "PGHOST" | base64decode}}
PGPORT={{index .data "PGPORT" | base64decode}}
PGUSER={{index .data "PGUSER" | base64decode}}
PGPASSWORD={{index .data "PGPASSWORD" | base64decode}}
PGDATABASE={{index .data "PGDATABASE" | base64decode}}
' > postgresql-secret.env

# Dashboard image
oc get deployment fournos-dashboard -n fournos-dashboard \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="dashboard")].image}'

# Storage class
oc get pvc -n fournos-dashboard -o jsonpath='{.items[0].spec.storageClassName}'
```


## Configuration

All configuration is via environment variables (set in the deployment manifest):

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string (required) | *none -- must be set* |
| `FOURNOS_NAMESPACE` | Namespace where FournosJobs run | *set via overlay* |
| `PROJECTS_CONFIG_PATH` | Path to projects YAML | `/etc/fournos-dashboard/projects.yaml` |
| `K8S_REQUEST_TIMEOUT` | Timeout for K8s API calls (seconds) | `30` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `KUBECONFIG` | Path to kubeconfig (local dev only) | in-cluster config |
| `FORGE_GITHUB_REPO` | GitHub `owner/repo` for PR listing | `openshift-psap/forge` |
| `RHAIIS_CONFIG_CACHE_TTL_SECONDS` | How often the dashboard checks Forge's default-branch SHA for RHAIIS config updates | `300` (5 minutes) |

## Security

Authentication is handled by the **OpenShift OAuth proxy** sidecar container. The proxy intercepts all requests to the Route, redirects unauthenticated users to the OpenShift login page, and only forwards traffic to the FastAPI app after successful authentication.

- **Who can access:** Any user who can authenticate to the OpenShift cluster.
- **TLS:** The Route uses a Let's Encrypt certificate (auto-renewed by cert-manager). Traffic between the Route and the pod is re-encrypted using a service-ca cert.
- **Local dev bypass:** When developing locally or using `oc port-forward` to port 8000, the OAuth proxy is bypassed entirely (traffic goes directly to FastAPI).





## Local Development

```bash
pip install -r requirements.txt

# Set DATABASE_URL and KUBECONFIG, then:
uvicorn app.main:app --reload --port 8000
```

## Project Structure

```
fournos-ui/
├── app/
│   ├── main.py              # FastAPI routes and Jinja2 rendering
│   ├── config.py            # Environment-based settings
│   ├── db.py                # SQLAlchemy models and queries
│   ├── k8s_client.py        # Kubernetes API wrapper (with timeouts)
│   ├── watcher.py           # Background FournosJob event watcher
│   ├── forge_discovery.py   # Project discovery from ConfigMap
│   ├── models.py            # Pydantic/dataclass models
│   ├── static/              # CSS, HTMX, htmx-sse.js
│   └── templates/           # Jinja2 HTML templates
├── kustomize/
│   ├── base/                # Generic K8s manifests
│   └── overlays/ocp/        # OpenShift deployment overlay
│       ├── kustomization.yaml
│       ├── dashboard-route.yaml            # Route with cert-manager annotations
│       ├── dashboard-certificate.yaml      # Let's Encrypt Certificate CR
│       ├── letsencrypt-clusterissuer.yaml  # ACME ClusterIssuer (apply separately)
│       ├── oauth-cookie-secret.yaml        # OAuth proxy session secret
│       ├── patch-deployment-oauth-proxy.yaml  # Adds OAuth sidecar to Deployment
│       ├── patch-service-oauth.yaml        # Adds TLS port to Service
│       ├── patch-serviceaccount-oauth.yaml # Adds OAuth redirect annotation
│       ├── projects.yaml                   # (user-created) project config
│       └── postgresql-secret.env           # (user-created) DB credentials
├── Dockerfile
└── requirements.txt
```
