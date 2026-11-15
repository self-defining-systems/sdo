package kubebreak

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/memory/appmap"
)

// checkLabel marks every object the check creates, so a leftover of an
// interrupted check is found and removed.
const checkLabel = "sdo.dev/onboarding-check"

// Fault names.
const (
	WorkerQuietName = "worker-quiet"
	DataWrongName   = "data-wrong"
)

// WorkerQuiet keeps a worker Running and Ready but denies its egress except
// DNS, so it cannot reach what it works on. It acts through a NetworkPolicy and
// so needs a CNI that enforces them.
type WorkerQuiet struct {
	b         *Breaker
	component appmap.Component
	ops       []string
}

// WorkerQuiet returns the worker-quiet fault for a worker component and the
// operations it hurts.
func (b *Breaker) WorkerQuiet(c appmap.Component, ops []string) *WorkerQuiet {
	return &WorkerQuiet{b: b, component: c, ops: ops}
}

func (f *WorkerQuiet) Name() string { return WorkerQuietName + ":" + f.component.Name }
func (f *WorkerQuiet) Describe() string {
	return fmt.Sprintf("NetworkPolicy %s denies all egress of %s except DNS (it stays Running and Ready)", f.policy(), f.component.Workload)
}
func (f *WorkerQuiet) Operations() []string { return f.ops }
func (f *WorkerQuiet) policy() string {
	return policyPrefix + "worker-egress-" + bare(f.component.Workload)
}

func (f *WorkerQuiet) NotApplicable(context.Context) string {
	if !f.b.enforced {
		return "this cluster's CNI does not enforce NetworkPolicy, so the worker's egress cannot be denied; run the check on a cluster whose CNI does"
	}
	return ""
}

func (f *WorkerQuiet) Break(ctx context.Context) error {
	w, err := f.b.find(ctx, f.component.Workload)
	if err != nil {
		return err
	}
	if w.selector == nil || len(w.selector.MatchLabels) == 0 {
		return fmt.Errorf("workload %s has no matchLabels selector to deny egress from", f.component.Workload)
	}
	dns := intstr.FromInt32(53)
	udp, tcp := corev1.ProtocolUDP, corev1.ProtocolTCP
	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: f.policy(), Namespace: f.b.cfg.Namespace, Labels: map[string]string{checkLabel: "true"}},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: w.selector.MatchLabels},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress:      []networkingv1.NetworkPolicyEgressRule{{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}}}},
		},
	}
	policies := f.b.cfg.Client.NetworkingV1().NetworkPolicies(f.b.cfg.Namespace)
	if err := policies.Delete(ctx, policy.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	_, err = policies.Create(ctx, policy, metav1.CreateOptions{})
	return err
}

func (f *WorkerQuiet) Restore(ctx context.Context) error {
	policies := f.b.cfg.Client.NetworkingV1().NetworkPolicies(f.b.cfg.Namespace)
	if err := policies.Delete(ctx, f.policy(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("restore %s: delete NetworkPolicy: %w", f.component.Workload, err)
	}
	if _, err := policies.Get(ctx, f.policy(), metav1.GetOptions{}); err == nil {
		return fmt.Errorf("restore %s: NetworkPolicy %s still exists", f.component.Workload, f.policy())
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("restore %s: %w", f.component.Workload, err)
	}
	return nil
}

// Exec runs a command in a container with the given standard input and returns
// its standard output.
type Exec func(ctx context.Context, namespace, pod, container string, command []string, stdin string) (string, error)

// DataWrong overwrites a text column of the oldest rows of the most-read table
// of a Postgres store while every component stays up, and restores the rows by
// primary key. The originals are kept in a table of the store itself, so a
// restore works after the check process died.
type DataWrong struct {
	b         *Breaker
	component appmap.Component
	ops       []string
	exec      Exec
}

// DataWrong returns the data-wrong fault for the authoritative store component.
func (b *Breaker) DataWrong(c appmap.Component, ops []string, exec Exec) *DataWrong {
	return &DataWrong{b: b, component: c, ops: ops, exec: exec}
}

func (f *DataWrong) Name() string { return DataWrongName }
func (f *DataWrong) Describe() string {
	return fmt.Sprintf("overwrites a text column of the oldest rows of the most-read table of %s (originals saved in %s, restored by primary key)", f.component.Name, backupTable)
}
func (f *DataWrong) Operations() []string { return f.ops }

func (f *DataWrong) NotApplicable(context.Context) string {
	switch {
	case f.exec == nil:
		return "this process cannot run commands in the store's pod"
	case f.component.Data == nil || f.component.Data.Engine != appmap.EnginePostgres:
		return fmt.Sprintf("only a postgres store can be made wrong by this check (%s declares another engine)", f.component.Name)
	}
	return ""
}

func (f *DataWrong) Break(ctx context.Context) error {
	out, err := f.run(ctx, "break")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "sdo-wrong ") {
		return fmt.Errorf("no row was changed in %s: %s", f.component.Name, strings.TrimSpace(out))
	}
	return nil
}

func (f *DataWrong) Restore(ctx context.Context) error {
	out, err := f.run(ctx, "restore")
	if err != nil {
		return fmt.Errorf("restore %s data: %w", f.component.Name, err)
	}
	if !strings.Contains(out, "sdo-restored ") {
		return fmt.Errorf("restore %s data: no confirmation: %s", f.component.Name, strings.TrimSpace(out))
	}
	return nil
}

var postgresImage = regexp.MustCompile(`(^|/)(postgres|postgis|pgvector)[^/]*([:@]|$)`)

func (f *DataWrong) run(ctx context.Context, mode string) (string, error) {
	w, err := f.b.find(ctx, f.component.Workload)
	if err != nil {
		return "", err
	}
	if w.selector == nil || len(w.selector.MatchLabels) == 0 {
		return "", fmt.Errorf("workload %s has no matchLabels selector to find its pod", f.component.Workload)
	}
	pods, err := f.b.cfg.Client.CoreV1().Pods(f.b.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(w.selector)})
	if err != nil {
		return "", err
	}
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		container := ""
		for _, c := range p.Spec.Containers {
			if postgresImage.MatchString(c.Image) {
				container = c.Name
			}
		}
		if container == "" && len(p.Spec.Containers) > 0 {
			container = p.Spec.Containers[0].Name
		}
		return f.exec(ctx, f.b.cfg.Namespace, p.Name, container, []string{"sh", "-s", mode}, dataWrongScript)
	}
	return "", errors.New("no running pod of " + f.component.Workload)
}

const backupTable = "public.sdo_onboarding_check_backup"

// dataWrongScript runs inside the store's container with the container's own
// credentials; $1 is "break" or "restore". The break runs in one transaction,
// so a failure leaves nothing changed.
const dataWrongScript = `set -eu
export PGUSER="${PGUSER:-${POSTGRES_USER:-postgres}}"
if [ -z "${PGPASSWORD:-}" ] && [ -n "${POSTGRES_PASSWORD:-}" ]; then export PGPASSWORD="$POSTGRES_PASSWORD"; fi
D="${POSTGRES_DB:-}"
if [ -z "$D" ]; then D=$(psql -X -q -At -d postgres -c "select datname from pg_database where not datistemplate and datname <> 'postgres' order by pg_database_size(datname) desc limit 1"); fi
q(){ psql -X -q -At -v ON_ERROR_STOP=1 -d "$D" "$@"; }
case "$1" in
break)
q <<'SQL'
create table if not exists public.sdo_onboarding_check_backup (tbl text, pk text, col text, k text, v text);
DO $$
declare r record; maxlen int;
begin
  perform 1 from public.sdo_onboarding_check_backup limit 1;
  if found then raise exception 'sdo-already-broken: an earlier check left rows to restore'; end if;
  select format('%I.%I', n.nspname, c.relname) as tbl, a.attname as pk, t.attname as col,
         case when t.atttypid = 1043 and t.atttypmod > 0 then t.atttypmod - 4 else 60 end as maxlen
    into r
    from pg_stat_user_tables s
    join pg_class c on c.oid = s.relid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_index i on i.indrelid = c.oid and i.indisprimary and i.indnatts = 1
    join pg_attribute a on a.attrelid = c.oid and a.attnum = i.indkey[0]
    join pg_attribute t on t.attrelid = c.oid and t.attnum > 0 and not t.attisdropped and t.atttypid in (25, 1043) and t.attname <> a.attname
   where s.n_live_tup > 0 and c.relname <> 'sdo_onboarding_check_backup'
   order by (s.seq_tup_read + coalesce(s.idx_tup_fetch, 0)) desc, t.attnum
   limit 1;
  if r.tbl is null then raise exception 'sdo-no-table: no table with a primary key, rows and a text column'; end if;
  execute format('insert into public.sdo_onboarding_check_backup select %L, %L, %L, %I::text, %I from %s order by %I limit 40', r.tbl, r.pk, r.col, r.pk, r.col, r.tbl, r.pk);
  execute format('update %s set %I = left(%L || %I::text, %s) where %I::text in (select k from public.sdo_onboarding_check_backup where tbl = %L)', r.tbl, r.col, 'sdo-check-wrong-', r.pk, r.maxlen, r.pk, r.tbl);
end
$$;
select 'sdo-wrong ' || tbl || ' ' || col || ' ' || count(*) from public.sdo_onboarding_check_backup group by tbl, col;
SQL
;;
restore)
q <<'SQL'
DO $$
declare r record;
begin
  if to_regclass('public.sdo_onboarding_check_backup') is null then return; end if;
  for r in select distinct tbl, pk, col from public.sdo_onboarding_check_backup loop
    execute format('update %s t set %I = b.v from public.sdo_onboarding_check_backup b where b.tbl = %L and t.%I::text = b.k', r.tbl, r.col, r.tbl, r.pk);
  end loop;
  drop table public.sdo_onboarding_check_backup;
end
$$;
select 'sdo-restored ok';
SQL
;;
*) echo "unknown mode $1" >&2; exit 2;;
esac
`
