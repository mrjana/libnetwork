package drvregistry

import (
	"testing"

	"github.com/docker/libnetwork/types"
)

var ntypes = []string{
	"bridge",
	"host",
	"null",
	"remote",
	"overlay",
}

func TestNewDriverRegsitry(t *testing.T) {
	_, err := NewDriverRegistry(ntypes)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewDriverRegsitryTwice(t *testing.T) {
	_, err := NewDriverRegistry(ntypes)
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewDriverRegistry(ntypes)
	if err != nil {
		t.Fatal(err)
	}
}

func testDriverFindAndCheckScope(name string, scope string, t *testing.T) {
	dr, err := NewDriverRegistry(ntypes)
	if err != nil {
		t.Fatal(err)
	}

	d, err := dr.Find(name)
	if err != nil {
		t.Fatal(err)
	}

	if d == nil {
		t.Fatal("Returned a nil interface while finding %q driver", name)
	}

	s, err := dr.Scope(name)
	if err != nil {
		t.Fatal(err)
	}

	if s != scope {
		t.Fatal("Returned scope %q does not match the expected scope %s for driver %s", s, scope, name)
	}
}

func TestDriverBuiltins(t *testing.T) {
	for _, d := range []struct {
		name  string
		scope string
	}{
		{"bridge", "local"},
		{"overlay", "global"},
		{"host", "local"},
		{"null", "local"},
	} {
		testDriverFindAndCheckScope(d.name, d.scope, t)
	}
}

func TestInvalidDriver(t *testing.T) {
	dr, err := NewDriverRegistry(ntypes)
	if err != nil {
		t.Fatal(err)
	}

	_, err = dr.Find("invalid")
	if err == nil {
		t.Fatal("expected to fail finding invalid driver but succeeded")
	}

	if err != nil {
		if _, ok := err.(types.NotFoundError); !ok {
			t.Fatalf("Invalid driver test failed due to wrong reason: %v", err)
		}
	}
}
