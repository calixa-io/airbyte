Create/update Docker image
===

1. `docker build -t kubectl-node:latest .`
2. `docker tag kubectl-node:latest gcr.io/calixa-cloudbuild-5a1f/kubectl-node:latest`
3. `docker push gcr.io/calixa-cloudbuild-5a1f/kubectl-node:latest`

Install Cron on Kubernetes
===
```shell
kubectl -n airbyte --dry-run=server apply -f ../airbyte-kill-stuck-pods.yaml
```
