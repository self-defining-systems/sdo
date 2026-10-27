# Hotel Reservation source deployment

SREGym's hotel source lives in its pinned `SREGym-applications/hotelReservation`
submodule, rather than the separate `apps/deathstarbench` checkout. The SDO
`third_party/sregym` gitlink pins the harness and its nested application pin.

The application's `kubernetes/kustomization.yaml` declares the full manifest set
and two stable-name ConfigMap generators, `mongo-geo-script` and
`mongo-rate-script`. Each generator reads its single tracked
`kubernetes/scripts/k8s-{geo,rate}-mongo.sh` input. Those scripts moved from the
application root without content changes. The MongoDB Deployments retain their
existing ConfigMap references. Deploy and delete with `kubectl apply -k
kubernetes` and `kubectl delete -k kubernetes`.

The harness stages `kubernetes/` with its scripts and preserves any existing
Kustomization. It synthesizes a resource list only for legacy sources without
one. Bootstrap ConfigMaps are no longer written separately by the harness;
benchmark failure-script ConfigMaps retain their independent lifecycle.
DEP1's accepted-source renderer uses the same declarations at the accepted Git
revision, so source recovery can recreate a deleted bootstrap ConfigMap.

## Existing lanes

Fresh hotel seed: `vibesys-playground/hotel-reservation-sdo`, branch
`app3-source`, commit `04ff0f6` (parent `afe37c1`). It adds the declaration and
relocates the original scripts; the historical manifests and `.sdo/` memory are
unchanged. Fresh workspaces also receive the declaration from the nested
application pin. Advancing the harness pin alone does not update a lane's
accepted Git source.

Adopt the new seed and matching harness only at a **problem-set boundary**.
Never change source, seed, harness or sealed baseline between a first try and
its repeat. The lane owner must finish the current set, deploy from the new
accepted seed, recapture the installed baseline, and record its new
`seed_commit` before starting the next set. Preserve prior results. A plain
reset reuses the old sealed baseline and is insufficient. Existing learned
memory lines need an explicitly prepared descendant with this source change;
replacing their head with the fresh seed would discard learning.

Validation renders the real pinned application through harness staging and
Kustomize, then through the production accepted-Git renderer. Both ConfigMaps
must contain exactly the tracked script bytes, and accepted restoration must
retain those bytes when live state is missing and working files are modified.
A lane's first deployment after adoption must additionally verify Pod readiness
and delete/restore one ConfigMap through `CreateFromSource` before a new live
problem set; offline rendering does not establish cluster readiness.
