#!/usr/bin/env bash
# Apply existing Fournos manifests (envsubst) plus this overlay's new objects.
# Does not apply config/kueue-cluster-config.yaml (dummy flavors) or Forge
# workflows (those come from forge/fournos/gitops).
set -euo pipefail

OVERLAY_DIR="$(cd "$(dirname "$0")" && pwd)"
FOURNOS_ROOT="$(cd "${OVERLAY_DIR}/../../.." && pwd)"
OC="${OC:-$(command -v oc || command -v kubectl)}"

CONTROLLER_NAMESPACE="${FOURNOS_CONTROLLER_NAMESPACE:-fournos-controller}"
FOURNOS_WORKLOAD_NAMESPACE="${FOURNOS_WORKLOAD_NAMESPACE:-psap-automation}"
FOURNOS_SECRETS_NAMESPACE="${FOURNOS_SECRETS_NAMESPACE:-psap-secrets}"

cd "${FOURNOS_ROOT}"

echo "Applying Intlab overlay (namespaces, ImageStream, ClusterQueue bootstrap)..."
"${OC}" apply -k "${OVERLAY_DIR}"

echo "Applying existing RBAC + Deployment..."
"${OC}" apply -f manifests/rbac/sa_fournos.yaml -n "${CONTROLLER_NAMESPACE}"
"${OC}" apply -f manifests/rbac/sa_fournos.yaml -n "${FOURNOS_WORKLOAD_NAMESPACE}"
for rbac_file in manifests/rbac/role_fournos.yaml manifests/rbac/rolebinding_fournos.yaml; do
  CONTROLLER_NAMESPACE="${CONTROLLER_NAMESPACE}" envsubst '$CONTROLLER_NAMESPACE' < "${rbac_file}" \
    | "${OC}" apply -f- -n "${FOURNOS_WORKLOAD_NAMESPACE}"
done
"${OC}" apply -f manifests/rbac/clusterrole_fournos.yaml
CONTROLLER_NAMESPACE="${CONTROLLER_NAMESPACE}" envsubst '$CONTROLLER_NAMESPACE' \
  < manifests/rbac/clusterrolebinding_fournos.yaml | "${OC}" apply -f-
CONTROLLER_NAMESPACE="${CONTROLLER_NAMESPACE}" SECRETS_NAMESPACE="${FOURNOS_SECRETS_NAMESPACE}" envsubst \
  < manifests/secrets-ns-rbac.yaml | "${OC}" apply -f-
NAMESPACE="${FOURNOS_WORKLOAD_NAMESPACE}" envsubst '$NAMESPACE' < manifests/deployment.yaml \
  | "${OC}" apply -f- -n "${CONTROLLER_NAMESPACE}"

echo "Applying existing LocalQueue + OCPCI SA..."
"${OC}" apply -f config/kueue-config.yaml -n "${FOURNOS_WORKLOAD_NAMESPACE}"
"${OC}" apply -f manifests/ocpci-sa/sa.yaml
"${OC}" apply -f manifests/ocpci-sa/role.yaml
"${OC}" apply -f manifests/ocpci-sa/rolebinding.yaml
"${OC}" apply -f manifests/ocpci-sa/secret.yaml

echo "Done."
