package secretrotation

import (
	"fmt"
	"strings"
	"sync"
)

// SecretKey uniquely identifies a secret in a namespace.
type SecretKey struct {
	Namespace string
	Name      string
}

func secretKey(namespace, name string) SecretKey {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}
	return SecretKey{Namespace: ns, Name: strings.TrimSpace(name)}
}

// Registry tracks secret versions and workloads that reference them.
type Registry struct {
	mu sync.RWMutex
	// versions maps secret key -> last observed version string
	versions map[SecretKey]string
	// workloads maps secret key -> workload IDs
	workloads map[SecretKey]map[string]struct{}
}

// NewRegistry creates an empty secret rotation registry.
func NewRegistry() *Registry {
	return &Registry{
		versions:  make(map[SecretKey]string),
		workloads: make(map[SecretKey]map[string]struct{}),
	}
}

// RegisterWorkload links secret refs from a workload spec.
func (r *Registry) RegisterWorkload(workloadID, namespace string, refs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range refs {
		if name == "" {
			continue
		}
		k := secretKey(namespace, name)
		if r.workloads[k] == nil {
			r.workloads[k] = make(map[string]struct{})
		}
		r.workloads[k][workloadID] = struct{}{}
	}
}

// WorkloadsForSecret returns workload IDs affected by a secret change.
func (r *Registry) WorkloadsForSecret(namespace, name string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	k := secretKey(namespace, name)
	set := r.workloads[k]
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

// ObserveVersion returns true when the version changed from the last observed value.
func (r *Registry) ObserveVersion(namespace, name, version string) (changed bool, prev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := secretKey(namespace, name)
	prev = r.versions[k]
	if prev == version {
		return false, prev
	}
	r.versions[k] = version
	return prev != "", prev
}

// CurrentVersion returns the last recorded version for a secret.
func (r *Registry) CurrentVersion(namespace, name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.versions[secretKey(namespace, name)]
}

// KeyString returns a stable string key for logging.
func (k SecretKey) String() string {
	return fmt.Sprintf("%s/%s", k.Namespace, k.Name)
}
