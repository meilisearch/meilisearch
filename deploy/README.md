# Deploying on Kubernetes

These manifests will deploy a basic MeiliSearch instance in development mode.

## Prerequisits

1. kubectl

   Needed for applying these manifests to the cluster.

2. A Kubernetes Cluster

   Any will do. For local development there are a number of options. See the [install page](https://kubernetes.io/docs/setup/) for all of them.
   If you already have docker installed then [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) is a good option.

   Simply run `kind create cluster` and wait until it's booted. The first time can take a while as it needs to download the kubernetes node container image.

3. Optionally kustomize

   Kustomize can be used to make layered adjustments to the manifests. It can also generate config maps for you that will trigger a redeploy.

   Download it [here](https://kustomize.io/).

## Applying the manifests

Applying the base manifests will result in:
- a serviceaccount
- a deployment
- a service
- a persistentvolumeclaim

Note that no configmap or secret is created which can contain environment variables and the master key.
To configure meilisearch either add your own configmap named meilisearch or use the kustomization.yaml file
to generate the configmap.

### kubectl

The manifests currently kan be applied as is using just kubectl.

```bash
$ kubectl apply -f ./base
deployment.apps/meilisearch created
persistentvolumeclaim/meilisearch created
service/meilisearch created
serviceaccount/meilisearch created
```

### kustomize

```bash
$ kustomize build ./base | kubectl apply -f -
serviceaccount/meilisearch created
configmap/meilisearch-9d4cg84854 created
service/meilisearch created
deployment.apps/meilisearch created
persistentvolumeclaim/meilisearch created
```

Note that in this case a configmap is created because the kustomization.yaml contains a configmap generator with some values set.

The directory in-memory contains an example of a patch that kustomize can apply which removes the persistent volume claim and
replaces the volume in the pod with an in memory volume. Just point kustomize to the directory to apply it.
It also contains a secret generator that sets a master key and alters the configmap to change the MEILI_ENV to production.

```bash
$ kustomize build ./in-memory | kubectl apply -f -
```

## Test the deployment

To see if it's started properly and is serving requests you kan view the logs using:

```bash
$ kubectl logs -f deployment/meilisearch

888b     d888          d8b 888 d8b  .d8888b.                                    888
8888b   d8888          Y8P 888 Y8P d88P  Y88b                                   888
88888b.d88888              888     Y88b.                                        888
888Y88888P888  .d88b.  888 888 888  "Y888b.    .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888     "Y88b. d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888       "888 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888 Y88b  d88P Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  "Y8888P"   "Y8888  "Y888888 888     "Y8888P 888  888

Database path:          "/data.ms"
Server listening on:    "0.0.0.0:7700"
Environment:            "development"
Commit SHA:             "UNKNOWN"
Build date:             "2020-06-25T13:42:08.261389882+00:00"
Package version:        "0.11.1"

No master key found; The server will accept unidentified requests. If you need some protection in development mode, please export a key: export MEILI_MASTER_KEY=xxx

Documentation:          https://docs.meilisearch.com
Source code:            https://github.com/meilisearch/meilisearch
Contact:                https://docs.meilisearch.com/resources/contact.html or bonjour@meilisearch.com

[2020-06-30T15:34:39Z INFO  actix_server::builder] Starting 1 workers
[2020-06-30T15:34:39Z INFO  actix_server::builder] Starting "actix-web-service-0.0.0.0:7700" service on 0.0.0.0:7700
[2020-06-30T15:34:44Z INFO  actix_web::middleware::logger] 10.244.0.1:58184 "GET /health HTTP/1.1" 200 0 "-" "kube-probe/1.18" 0.000650
```

To connect to it you can port-forward port 7700 to localhost:

```bash
$ kubectl port-forward service/meilisearch 7700
```

Now you can simply go to http://localhost:7700 in your browser or use curl.