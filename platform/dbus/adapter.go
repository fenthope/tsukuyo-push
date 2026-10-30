// Package dbusadapter implements the UnifiedPush D-Bus distributor API
// (spec DBUS_0.3.0): it serves org.unifiedpush.Distributor2 on the
// session bus, drives a dcore.Distributor, and calls back into
// org.unifiedpush.Connector2 on the applications' bus names.
//
// It is its own module so that godbus stays out of the core library's
// dependency graph; depending on this package is the opt-in.
package dbusadapter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"

	"github.com/satomitoka/tsukuyo-push/dcore"
)

const (
	// DistributorIface is the UnifiedPush distributor interface.
	DistributorIface = "org.unifiedpush.Distributor2"
	// DistributorPath is the object path of the distributor.
	DistributorPath = "/org/unifiedpush/Distributor"
	// DistributorNamePrefix prefixes the well-known bus name.
	DistributorNamePrefix = "org.unifiedpush.Distributor."
	// ConnectorIface and ConnectorPath address an application.
	ConnectorIface = "org.unifiedpush.Connector2"
	ConnectorPath  = "/org/unifiedpush/Connector"

	// DefaultName is the distributor name used when none is configured.
	DefaultName = "tsukuyo"

	registerTimeout  = 30 * time.Second
	connectorTimeout = 15 * time.Second
)

// Adapter drives the D-Bus surface: it implements dcore.Dispatcher for
// deliveries and exposes Distributor2 through Serve.
type Adapter struct {
	conn *dbus.Conn
	dist *dcore.Distributor
	name string
	log  *slog.Logger
}

// New creates the adapter. Wire it up in this order: build the adapter,
// hand adapter.Deliver to dcore.NewDistributor as a dcore.DispatcherFunc,
// then Attach the distributor and Serve.
func New(conn *dbus.Conn, name string, log *slog.Logger) (*Adapter, error) {
	if name == "" {
		name = DefaultName
	}
	busName := DistributorNamePrefix + name
	if !validBusName(busName) {
		return nil, fmt.Errorf("dbusadapter: invalid distributor name %q", busName)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Adapter{conn: conn, name: busName, log: log}, nil
}

// Attach links the distributor driven by this adapter. It must be called
// before Serve.
func (a *Adapter) Attach(dist *dcore.Distributor) { a.dist = dist }

// Serve exports the Distributor2 object and claims the bus name.
func (a *Adapter) Serve() error {
	if a.dist == nil {
		return errors.New("dbusadapter: distributor not attached")
	}
	if err := a.conn.Export(&distributorAPI{a}, dbus.ObjectPath(DistributorPath), DistributorIface); err != nil {
		return fmt.Errorf("dbusadapter: export: %w", err)
	}
	reply, err := a.conn.RequestName(a.name, dbus.NameFlagDoNotQueue)
	if err != nil {
		return fmt.Errorf("dbusadapter: request name %s: %w", a.name, err)
	}
	if reply != dbus.RequestNameReplyPrimaryOwner {
		return fmt.Errorf("dbusadapter: name %s already taken", a.name)
	}
	a.log.Info("distributor ready on session bus", "name", a.name)
	return nil
}

// NotifyGone wires into dcore Options.OnGone: the push server dropped the
// subscription, so the connector is told to re-register.
func (a *Adapter) NotifyGone(reg dcore.Registration) {
	if reg.Application == "" {
		return
	}
	go a.callConnector(reg.Application, "Unregistered", map[string]dbus.Variant{
		"token": dbus.MakeVariant(reg.ConnectionToken),
	})
}

// Deliver implements dcore.Dispatcher: relay the encrypted push message to
// the connector identified by the registration's bus name. D-Bus service
// activation may wake the application; waiting for the method reply gives
// at-least-once delivery into the connector before the push is acked.
func (a *Adapter) Deliver(ctx context.Context, connectionToken string, body []byte) error {
	reg, ok := a.dist.Registration(connectionToken)
	if !ok || reg.Application == "" {
		return errors.New("dbusadapter: unknown connection token")
	}
	call := a.connectorObject(reg.Application).Call(
		ConnectorIface+".Message", 0, dbus.MakeVariant(map[string]dbus.Variant{
			"token":   dbus.MakeVariant(connectionToken),
			"message": dbus.MakeVariant(body),
		}))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-call.Done:
		return call.Err
	}
}

func (a *Adapter) callConnector(service, method string, args map[string]dbus.Variant) {
	ctx, cancel := context.WithTimeout(context.Background(), connectorTimeout)
	defer cancel()
	call := a.connectorObject(service).Call(ConnectorIface+"."+method, 0, dbus.MakeVariant(args))
	select {
	case <-ctx.Done():
		a.log.Warn("connector call timed out", "service", service, "method", method)
	case <-call.Done:
		if call.Err != nil {
			a.log.Warn("connector call failed",
				"service", service, "method", method, "err", call.Err)
		}
	}
}

func (a *Adapter) connectorObject(service string) dbus.BusObject {
	return a.conn.Object(service, dbus.ObjectPath(ConnectorPath))
}

// distributorAPI is the exported D-Bus surface; keeping it a separate
// type stops Adapter's other methods from leaking onto the bus.
type distributorAPI struct{ a *Adapter }

// Register implements org.unifiedpush.Distributor2.Register.
func (d distributorAPI) Register(args map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	a := d.a
	service, _ := lookup[string](args, "service")
	token, _ := lookup[string](args, "token")
	description, _ := lookup[string](args, "description")
	vapid, _ := lookup[string](args, "vapid")

	if token == "" || len(token) > 100 || !validBusName(service) {
		// Malformed calls are refused without touching the distributor.
		return map[string]dbus.Variant{
			"success": dbus.MakeVariant("REGISTRATION_FAILED"),
			"reason":  dbus.MakeVariant("INTERNAL_ERROR"),
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), registerTimeout)
	defer cancel()
	endpoint, err := a.dist.Register(ctx, token, service, description, vapid)
	if err != nil {
		a.log.Warn("registration failed", "service", service, "err", err)
		return map[string]dbus.Variant{
			"success": dbus.MakeVariant("REGISTRATION_FAILED"),
			"reason":  dbus.MakeVariant(reason(err)),
		}, nil
	}

	// The spec requires a NewEndpoint call for every successful Register,
	// including re-registration with an unchanged endpoint; it must not
	// block the reply on the connector.
	go a.callConnector(service, "NewEndpoint", map[string]dbus.Variant{
		"token":    dbus.MakeVariant(token),
		"endpoint": dbus.MakeVariant(endpoint),
	})
	return map[string]dbus.Variant{
		"success": dbus.MakeVariant("REGISTRATION_SUCCEEDED"),
	}, nil
}

// Unregister implements org.unifiedpush.Distributor2.Unregister.
func (d distributorAPI) Unregister(args map[string]dbus.Variant) (map[string]dbus.Variant, *dbus.Error) {
	a := d.a
	token, _ := lookup[string](args, "token")
	if token == "" {
		return map[string]dbus.Variant{}, nil
	}
	app, _ := a.busNameFor(token)
	ctx, cancel := context.WithTimeout(context.Background(), registerTimeout)
	defer cancel()
	if err := a.dist.Unregister(ctx, token); err != nil {
		a.log.Warn("unregister failed", "token", token, "err", err)
		return map[string]dbus.Variant{}, nil
	}
	if app != "" {
		go a.callConnector(app, "Unregistered", map[string]dbus.Variant{
			"token": dbus.MakeVariant(token),
		})
	}
	return map[string]dbus.Variant{}, nil
}

// busNameFor resolves a connection token to the application's bus name.
func (a *Adapter) busNameFor(token string) (string, bool) {
	reg, ok := a.dist.Registration(token)
	if !ok {
		return "", false
	}
	return reg.Application, reg.Application != ""
}

func lookup[T any](args map[string]dbus.Variant, key string) (T, bool) {
	var zero T
	v, ok := args[key]
	if !ok {
		return zero, false
	}
	typed, ok := v.Value().(T)
	return typed, ok
}

// validBusName checks the well-known bus name grammar (D-Bus spec 3.3):
// at least two elements of [A-Za-z_-][A-Za-z0-9_-]*, total length <= 255.
func validBusName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	elems := strings.Split(name, ".")
	if len(elems) < 2 {
		return false
	}
	for _, e := range elems {
		if e == "" {
			return false
		}
		for i := 0; i < len(e); i++ {
			c := e[i]
			ok := c == '_' || c == '-' ||
				(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(i > 0 && c >= '0' && c <= '9')
			if !ok {
				return false
			}
		}
	}
	return true
}

// reason maps a dcore error to the spec's registration failure reasons.
func reason(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr):
		return "NETWORK"
	default:
		return "INTERNAL_ERROR"
	}
}
