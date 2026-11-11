package runtime

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
)

// PlatformRefresh is how long one platform observation is reused before the
// namespace's policy objects are listed again.
const PlatformRefresh = 10 * time.Second

// PlatformState is the controller-lifetime state of the platform observation:
// the accepted source inventory reader (set once the controller exists) and
// the promoted healthy baseline. One holder outlives every memory-host swap.
type PlatformState struct {
	mu        sync.RWMutex
	inventory func() ([]sdk.ObjectRef, error)
	baseline  *settledBaseline[sdk.PlatformObservation]
}

// NewPlatformState returns a holder whose baseline promotions settle for settle.
func NewPlatformState(settle time.Duration) *PlatformState {
	return &PlatformState{baseline: newSettledBaseline[sdk.PlatformObservation](settle)}
}

// SetInventory sets the accepted source inventory reader.
func (s *PlatformState) SetInventory(read func() ([]sdk.ObjectRef, error)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.inventory = read
	s.mu.Unlock()
}

func (s *PlatformState) readInventory() ([]sdk.ObjectRef, error) {
	if s == nil {
		return nil, errors.New("accepted source inventory is unavailable")
	}
	s.mu.RLock()
	read := s.inventory
	s.mu.RUnlock()
	if read == nil {
		return nil, errors.New("accepted source inventory is unavailable")
	}
	return read()
}

// observe feeds one evaluation to the baseline.
func (s *PlatformState) observe(now time.Time, healthy bool, snapshot sdk.DetectionContext) {
	if s == nil {
		return
	}
	source, ok := snapshot.(sdk.PlatformSource)
	if !ok {
		return
	}
	observed, ok := source.PlatformObservation()
	if !ok {
		return
	}
	observed.Baseline = nil
	s.baseline.Observe(now, healthy, observed)
}

// PlatformObserver lists the application namespace's platform policy.
type PlatformObserver struct {
	Client             kubernetes.Interface
	ProtectedNamespace string
	State              *PlatformState
	Clock              func() time.Time

	mu   sync.Mutex
	last *sdk.PlatformObservation
}

func (o *PlatformObserver) observe(ctx context.Context, namespace string) sdk.PlatformObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.Clock().UTC()
	if o.last != nil && now.Sub(o.last.ObservedAt) < PlatformRefresh && o.last.Namespace.Name == namespace {
		return *o.last
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	observed := sdk.PlatformObservation{ObservedAt: now, Namespace: metav1.ObjectMeta{Name: namespace}, Errors: map[string]string{}}
	fail := func(part string, err error) { observed.Errors[part] = err.Error() }
	core := o.Client.CoreV1()
	if ns, err := core.Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		fail(sdk.PlatformNamespace, err)
	} else {
		observed.Namespace = metav1.ObjectMeta{Name: ns.Name, Labels: ns.Labels, Annotations: withoutBookkeeping(ns.Annotations)}
	}
	if list, err := core.LimitRanges(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		fail(sdk.PlatformLimitRanges, err)
	} else {
		for _, item := range list.Items {
			item.ObjectMeta = metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, Labels: item.Labels}
			observed.LimitRangeList = append(observed.LimitRangeList, item)
		}
		sort.Slice(observed.LimitRangeList, func(i, j int) bool { return observed.LimitRangeList[i].Name < observed.LimitRangeList[j].Name })
	}
	if list, err := core.ResourceQuotas(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		fail(sdk.PlatformResourceQuotas, err)
	} else {
		for _, item := range list.Items {
			item.ObjectMeta = metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, Labels: item.Labels}
			observed.QuotaList = append(observed.QuotaList, item)
		}
		sort.Slice(observed.QuotaList, func(i, j int) bool { return observed.QuotaList[i].Name < observed.QuotaList[j].Name })
	}
	rbac := o.Client.RbacV1()
	if list, err := rbac.Roles(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		fail(sdk.PlatformRoles, err)
	} else {
		for _, item := range list.Items {
			item.ObjectMeta = metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, Labels: item.Labels}
			observed.RoleList = append(observed.RoleList, item)
		}
		sort.Slice(observed.RoleList, func(i, j int) bool { return observed.RoleList[i].Name < observed.RoleList[j].Name })
	}
	if list, err := rbac.RoleBindings(namespace).List(ctx, metav1.ListOptions{}); err != nil {
		fail(sdk.PlatformRoleBindings, err)
	} else {
		for _, item := range list.Items {
			item.ObjectMeta = metav1.ObjectMeta{Name: item.Name, Namespace: item.Namespace, Labels: item.Labels}
			observed.BindingList = append(observed.BindingList, item)
		}
		sort.Slice(observed.BindingList, func(i, j int) bool { return observed.BindingList[i].Name < observed.BindingList[j].Name })
	}
	if webhooks, err := kubereads.NamespaceAdmissionWebhooks(ctx, o.Client, o.ProtectedNamespace, namespace); err != nil {
		fail(sdk.PlatformAdmissionWebhooks, err)
	} else {
		observed.Webhooks = webhooks
	}
	if inventory, err := o.State.readInventory(); err != nil {
		fail(sdk.PlatformSourceInventory, err)
	} else {
		observed.Inventory = inventory
	}
	if len(observed.Errors) == 0 {
		observed.Errors = nil
	}
	o.last = &observed
	return observed
}

// withoutBookkeeping drops last-applied configuration, which repeats the
// object, from observed annotations.
func withoutBookkeeping(annotations map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range annotations {
		if key != "kubectl.kubernetes.io/last-applied-configuration" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// platformSnapshotProvider adds the platform observation, with the promoted
// healthy observation as its baseline, to each snapshot.
type platformSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *PlatformObserver
}

type platformValue sdk.PlatformObservation

func (v platformValue) PlatformObservation() (sdk.PlatformObservation, bool) {
	return sdk.PlatformObservation(v), true
}

func (p platformSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return base, err
	}
	observed := p.Observer.observe(ctx, base.Namespace())
	if p.Observer.State != nil {
		if promoted, ok := p.Observer.State.baseline.Current(); ok {
			observed.Baseline = &promoted
		}
	}
	return inputs.New(base).WithPlatform(platformValue(observed)), nil
}

// AcceptedSourceRefs lists the objects the accepted source inventory
// declares, sorted, or an error while no inventory is loaded.
func (c *Controller) AcceptedSourceRefs() ([]sdk.ObjectRef, error) {
	c.mu.Lock()
	inventory := c.sourceInventory
	c.mu.Unlock()
	if inventory == nil {
		return nil, errors.New("accepted source inventory is not loaded")
	}
	refs := make([]sdk.ObjectRef, 0, len(inventory.declared))
	for ref := range inventory.declared {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return refs, nil
}
