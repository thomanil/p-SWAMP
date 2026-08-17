# p-swamp serverside: deploy & operate

Cheat sheet for the manifest in [p-swamp-rndp.yaml](p-swamp-rndp.yaml).
For now, run these ad-hoc from a terminal in your rndp notebook pod.

## Setup

kubectl in the notebook pod authenticates with **in-cluster config** (the
ServiceAccount token under `/var/run/secrets/kubernetes.io/serviceaccount/`).
There is no `~/.kube/config`, so:

- `kubectl config set-context ...` fails with `no current context is set` -- expected, not a fault.
- There is no default namespace, so every query command needs `-n`. The namespace is `rndp-p-swamp`.

Save typing with an alias (re-add after the notebook pod restarts):

```bash
alias kp='kubectl -n rndp-p-swamp'
```

All commands below are written out in full; substitute `kp` if the alias is set.

## Apply the manifest

No `--namespace` needed -- `metadata.namespace` is set on all three objects and
takes precedence over the flag.

```bash
kubectl apply -f k8s/p-swamp-rndp.yaml
```

Preview first (optional):

```bash
# does the namespace exist and do I have rights?
kubectl auth can-i create deployments -n rndp-p-swamp

# full admission check (RBAC + Kyverno webhooks), nothing persisted
kubectl apply -f k8s/p-swamp-rndp.yaml --dry-run=server

# unified diff against live state; exit code 1 just means "differs"
kubectl diff -f k8s/p-swamp-rndp.yaml
```

`--dry-run=client` only parses YAML locally -- it will not catch a missing
namespace, an RBAC failure or a policy rejection. Use `server`.

## Check deployment status

```bash
# blocks until the rollout completes; non-zero exit on timeout
kubectl -n rndp-p-swamp rollout status deployment/p-swamp --timeout=120s

# watch pods come up
kubectl -n rndp-p-swamp get pods -w

# everything at once
kubectl -n rndp-p-swamp get deploy,rs,pods,svc,ingress
```

When it will not come up, the **Events** section here explains almost everything
(image pull, scheduling, probe and admission failures):

```bash
kubectl -n rndp-p-swamp describe pod -l app=p-swamp

kubectl -n rndp-p-swamp get events --sort-by=.lastTimestamp
```

## Logs

`-c p-swamp` is needed because linkerd injects sidecar containers.

```bash
# follow
kubectl -n rndp-p-swamp logs -l app=p-swamp -c p-swamp -f

# last 50 lines, then follow
kubectl -n rndp-p-swamp logs -l app=p-swamp -c p-swamp --tail=50 -f

# last 10 minutes
kubectl -n rndp-p-swamp logs -l app=p-swamp -c p-swamp --since=10m

# after a crash-restart the real error is in the *previous* container
kubectl -n rndp-p-swamp logs -l app=p-swamp -c p-swamp --previous
```

No output at all means the container never started -- that is a `describe pod`
problem, not a logging one.

## Force redeploy when only the image changed

Kubernetes never watches the registry. When CI pushes a new `:latest` to ghcr the
image string in the Deployment is unchanged, so `kubectl apply` correctly does
nothing. The moved tag is only resolved when a new pod is created:

```bash
kubectl -n rndp-p-swamp rollout restart deployment/p-swamp
kubectl -n rndp-p-swamp rollout status deployment/p-swamp
```

This stamps a timestamp annotation on the pod template, forcing a new ReplicaSet
and a fresh pull (`:latest` implies `imagePullPolicy: Always`).

Confirm the new build actually landed by comparing the digest before/after:

```bash
kubectl -n rndp-p-swamp get pod -l app=p-swamp \
  -o jsonpath='{.items[*].status.containerStatuses[*].imageID}'
```

An unchanged digest while ghcr has moved means the pull was served from Harbor's
proxy cache -- Kubernetes has done its part at that point.

So: **push to GitHub -> wait for CI -> `rollout restart`**. `apply` is only needed
when the YAML itself changes.

## Verifying the request path

```bash
# did the Service find the pod? empty result = label selector mismatch (503 at the ingress)
kubectl -n rndp-p-swamp get endpointslices -l kubernetes.io/service-name=p-swamp

# did Kyverno rewrite ghcr.io -> harbor?
kubectl -n rndp-p-swamp get pod -l app=p-swamp \
  -o jsonpath='{.items[*].spec.containers[*].image}'
```

## Troubleshooting quick reference

| Symptom | Cause |
|---|---|
| `apply` rejected immediately | Namespace missing (rndp/auth chart not redeployed) or Kyverno denial |
| Pod `Pending` | Scheduling -- check `describe` for node/taint issues |
| Pod `ImagePullBackOff` | Harbor's `github` proxy-cache could not fetch the image |
| HTTP 503 | No endpoints -- Service selector does not match pod labels |
| HTTP 502 | Pod found but connection refused -- `containerPort` does not match the port the app listens on, or the app is bound to `127.0.0.1` instead of `0.0.0.0` |
| Page loads but assets 404 | The ingress strips the `/p-swamp` prefix, so absolute asset URLs miss. Fix in the app build (Vite `base`, FastAPI `root_path`), not in the manifest |

Note that `pods/exec` and `pods/portforward` are **not** granted by the `rndp-ops`
ClusterRole, so `kubectl exec` and `kubectl port-forward` are unavailable in the
project namespace.
