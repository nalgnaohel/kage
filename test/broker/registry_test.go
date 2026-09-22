package broker_test

import (
	"testing"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

func TestCreateLog_ThenGetLog_ReturnsSameLog(t *testing.T) {
	reg := newTestRegistry(t)

	created, err := reg.CreateLog("orders", 0)
	if err != nil {
		t.Fatalf("CreateLog() failed: %v", err)
	}

	got, ok := reg.GetLog("orders", 0)

	assertGolden(t, "registry", struct {
		GetOk   bool
		SameLog bool
	}{ok, got == created})
}

func TestGetLog_UnknownTopic_ReturnsFalse(t *testing.T) {
	reg := newTestRegistry(t)

	_, ok := reg.GetLog("missing", 0)

	assertGolden(t, "registry", struct{ Ok bool }{ok})
}

func TestCreateLog_AlreadyExists_ReturnsError(t *testing.T) {
	reg := newTestRegistry(t)

	_, firstErr := reg.CreateLog("orders", 0)
	_, secondErr := reg.CreateLog("orders", 0)

	assertGolden(t, "registry", struct {
		FirstErr  string
		SecondErr string
	}{errString(firstErr), errString(secondErr)})
}

func TestStartup_DiscoversPartitionsFromPreviousRun(t *testing.T) {
	dir := t.TempDir()

	first := broker.NewRegistry(dir, storage.DefaultLogConfig())
	if err := first.Startup(); err != nil {
		t.Fatalf("first Startup() failed: %v", err)
	}
	if _, err := first.CreateLog("orders", 0); err != nil {
		t.Fatalf("CreateLog() failed: %v", err)
	}
	if _, err := first.CreateLog("orders", 1); err != nil {
		t.Fatalf("CreateLog() failed: %v", err)
	}

	second := broker.NewRegistry(dir, storage.DefaultLogConfig())
	if err := second.Startup(); err != nil {
		t.Fatalf("second Startup() failed: %v", err)
	}

	_, p0Ok := second.GetLog("orders", 0)
	_, p1Ok := second.GetLog("orders", 1)

	assertGolden(t, "registry", struct {
		Partition0Found bool
		Partition1Found bool
	}{p0Ok, p1Ok})
}
