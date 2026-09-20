# ARC fork patches — listener resource-check opt-out (Task C3)

Patches against `ascend-gha-runners/actions-runner-controller` branch **`0.14.201`**
(head `8bdd51a`, the source of `swr.cn-southwest-2.myhuaweicloud.com/modelfoundry/gha-runner-scale-set-controller:0.14.201`).

| File | Commit | Touches |
|------|--------|---------|
| `0001-listener-resource-check-opt-out.patch` | `feat(listener): add ACTIONS_LISTENER_DISABLE_RESOURCE_CHECK opt-out` | `cmd/ghalistener/scaler/scaler.go` (+17/-1), `cmd/ghalistener/scaler/scaler_test.go` (+12) |
| `0002-fix-0.14.201-build.patch` | `fix(controller): make branch 0.14.201 compile again` | `controllers/actions.github.com/autoscalinglistener_controller.go` (+2/-1) |

Apply with `git am 0001-*.patch 0002-*.patch` on a checkout of `0.14.201`
(verified with `git apply --check` against the pristine tree; both apply cleanly and in any order).

`0002` is **not** part of the feature: branch head `8bdd51a` does not compile
(see "Pre-existing breakage" below), so nothing in `cmd/ghalistener/` could be
built or tested without it. Keep it separate when upstreaming.

## Why (design Addendum 2, implication 2)

The fork's listener gates every scale-up on live cluster capacity
(`cmd/ghalistener/scaler/resource_checker.go`, `AdjustCount`): it lists the
target Nodes, subtracts all Pod requests, divides by the per-runner request
taken from the `actions.github.com/job-*` annotations, and returns a capacity
that `scaler.HandleDesiredRunnerCount` uses as `maxRunners = min(capacity, configured)`.
At capacity 0 it returns `(0, nil)` and **no EphemeralRunner is ever created**
(`scaler.go:206-210` at branch head).

Under the Karmada design the listener talks to **karmada-apiserver**, which owns
the `EphemeralRunnerSet`/`EphemeralRunner` objects but **no Nodes**:
`Nodes().List` there succeeds and returns an *empty* list — not an error. So
allocatable sums to zero, `divideQuantity(0, req) == 0`, capacity is 0, and the
scale set is permanently stuck at zero runners even though the member clusters
are idle. None of the existing self-skip paths catch this (an empty list is not
`Forbidden`, and the ERS does carry the `job-*` annotations — that annotation
channel is what §3 of the design depends on).

With `ACTIONS_LISTENER_DISABLE_RESOURCE_CHECK=true`, `scaler.New` leaves
`resourceChecker` nil, `HandleDesiredRunnerCount` skips the whole block, and the
listener behaves like upstream 0.14.2. Admission then happens where it belongs
in the federated design: in the member clusters (scheduler/ARCSync), not in the
listener's cluster-wide sum over an empty node set.

The env var is read once in `scaler.New`, so it also wins over an injected
`WithResourceChecker` (test-only option). Unset, empty, `false`, `0` or an
unparsable value all keep today's behaviour — this is opt-in-to-disable,
zero impact on the 13 existing non-Karmada clusters.

### Setting it — no chart change needed

`mergeListenerContainer` (`controllers/actions.github.com/resourcebuilder.go:563-575`)
appends `listenerTemplate` env onto the listener container, so the values file of
the federated scale set carries it:

```yaml
gha-runner-scale-set:
  listenerTemplate:
    spec:
      containers:
        - name: listener
          env:
            - name: ACTIONS_LISTENER_DISABLE_RESOURCE_CHECK
              value: "true"
```

Intended repo path for that snippet: the runner chart values of the federated
scale set, e.g. `projects/<org>/<repo>/linux-aarch64-<npu>-<n>-<cluster>/values.yaml`.
The patches themselves are **not** deployment-repo content; they belong in the ARC fork.

## Alternative: deny `nodes`/`pods` list to the listener's control-plane identity

`AdjustCount` already self-skips (returns `math.MaxInt`, i.e. "unlimited") when
the node or pod list comes back `Forbidden` (`resource_checker.go:196-200` and
`216-220`). If the listener's karmada-apiserver credential is bound only to
ARC's `resourceName`-scoped namespace Role (design §2), the check disables
itself with no code change.

Why the patch is still preferable:

- It is implicit and fragile. The controller's `reconcileClusterRBAC`
  (`autoscalinglistener_controller.go:846+`) *actively tries* to create a
  ClusterRole granting the listener SA `nodes get,list`, `pods list`,
  `scheduling.volcano.sh/queues get` (`resourcebuilder.go:1068-1086`). The
  behaviour therefore depends on the controller's own CP permissions failing
  the way we expect; anyone who later grants the controller ClusterRole
  management on the CP silently re-enables the check and zeroes the scale set.
- The failure mode is silent and total (no runners), and the only signal is a
  `Warn` line in the listener log.
- It is all-or-nothing per identity, while the env var is per scale set — the
  same CP can host federated scale sets (check off) and, if ever needed,
  direct ones.

Belt and braces is fine: keep the least-privilege binding *and* set the env var.
Record whichever is chosen in the M0 runbook.

## What makes the check skip itself today (from `resource_checker.go`, branch 0.14.201)

`AdjustCount` returns `math.MaxInt` ("no constraint", later capped by the
configured `maxRunners`) in exactly these cases:

1. **No per-job requirement known** (`:180-183`) — the ERS has none of
   `actions.github.com/job-cpu`, `job-memory`, `job-npu` **and**
   `inferFromRunnerContainerLimits` finds no `resources.limits` on the container
   named `runner` in the ERS pod template.
2. **`nodes list` Forbidden** (`:196-200`) — only `kerrors.IsForbidden`; any
   other list error is returned as an error.
3. **`pods list` (all namespaces) Forbidden** (`:216-220`) — same rule.

The Volcano sub-check (`volcanoQueueCapacity`) skips independently, leaving the
node-based capacity in force:

4. **Volcano client nil** (`:297-299`) — `volcanoclient.NewForConfig` failed, or
   `NewKubernetesResourceChecker` got a nil rest config.
5. **No queue annotation** (`:302-305`) — the ERS pod template lacks
   `scheduling.volcano.sh/queue-name`.
6. **Queue NotFound** (`:310-312`) or **queue get Forbidden** (`:314-316`).
7. Per resource, a queue with no `spec.capability` entry for it is ignored (`:322-325`).

Additionally, on the caller side (`scaler.go:199-201` at branch head), any *error* from
`AdjustCount` is fail-open: logged as `Warn` and scaling proceeds with the
configured `maxRunners`. The one path that is not skippable is the one that
matters here — a successful list that returns **zero** nodes/allocatable, which
yields capacity 0 and a hard `(0, nil)`.

## Pre-existing breakage on branch head `8bdd51a`

```
controllers/actions.github.com/autoscalinglistener_controller.go:293:54:
  cannot use autoscalingListener (variable of struct type v1alpha1.AutoscalingListener)
  as *v1alpha1.AutoscalingListener value in argument to r.reconcileClusterRBAC
controllers/actions.github.com/autoscalinglistener_controller.go:293:75:
  cannot use serviceAccount (variable of struct type corev1.ServiceAccount) as *corev1.ServiceAccount
controllers/actions.github.com/autoscalinglistener_controller.go:865:22: undefined: hash
```

`reconcileClusterRBAC` takes pointers and `hash.ComputeTemplateHash` needs the
`github.com/actions/actions-runner-controller/hash` import. Since
`cmd/ghalistener/scaler` imports this package (for the `job-*` annotation
constants), neither the listener binary nor its tests build at branch head.
**The published image `:0.14.201` therefore cannot have been built from this
commit** — worth confirming with whoever built it which tree it came from; the
fork branch and the running image may differ in more than this.

## Validation performed (go1.24.4, local, no cluster)

```
$ go build ./cmd/ghalistener/...          # exit 0 (after 0002; fails at branch head)
$ go vet ./cmd/ghalistener/...            # exit 0
$ go test ./cmd/ghalistener/scaler/...    # ok  ...cmd/ghalistener/scaler  0.048s
$ gofmt -l cmd/ghalistener/scaler/scaler.go cmd/ghalistener/scaler/scaler_test.go   # clean
```

`TestResourceCheckDisabled` asserts unset/`false`/`0`/``/`bogus` keep the check
on and `true`/`1`/`TRUE` turn it off. Pre-existing
`resource_checker.go` is unformatted at branch head (`gofmt -l` flags it,
import block ordering) and is left untouched.

Scratch build tree: `../arc-patch-work` (copy of `.tmp/arc-src-0.14.201`; the
original was not modified, `git status` there is clean).
