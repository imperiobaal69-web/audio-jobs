#!/usr/bin/env bash
# Validate the Kubernetes manifests against the API schemas (kubeconform).
# CRDs without a published schema (KEDA) are reported as skipped.
set -euo pipefail
cd "$(dirname "$0")/.."
kubeconform -strict -summary -kubernetes-version 1.31.0 -ignore-missing-schemas \
  -skip Kustomization deploy/k8s/*.yaml
