package dcore

import "time"

// Registration binds one end user application (via the distributor's
// connection token) to its Web Push resources.
//
// Registrations travel by value: the distributor keeps its own copy, and
// everything it hands out (Registrations, Registration, the change events)
// is a copy too, so an embedding application can persist, mutate or cache
// them freely.
type Registration struct {
	// ConnectionToken identifies the app's registration with the
	// distributor (UnifiedPush spec). Every delivery carries it.
	ConnectionToken string `json:"connection_token"`
	// Application is the platform identity of the end user application:
	// the D-Bus bus name on Linux, the package name on Android. The
	// adapter uses it to address the connector.
	Application string `json:"application,omitempty"`
	// Label is a short description of the registration the distributor
	// may show to the user.
	Label string `json:"label,omitempty"`

	WebSubscription
	CreatedAt time.Time `json:"created_at"`
}

// ChangeKind distinguishes the persistence events reported through
// Options.OnChanged.
type ChangeKind int

const (
	// Registered means a registration was added or replaced.
	Registered ChangeKind = iota
	// Removed means the distributor no longer polls the registration:
	// the application unregistered, or the push server reported the
	// subscription as gone.
	Removed
)

// String names the kind for logs.
func (k ChangeKind) String() string {
	if k == Removed {
		return "removed"
	}
	return "registered"
}
