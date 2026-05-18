package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/HeaInSeo/spawner/pkg/api"
	"github.com/HeaInSeo/spawner/pkg/dispatcher"
)

func TestRunStorePath_DefaultsToTmp(t *testing.T) {
	path := runStorePath(func(string) string { return "" })
	if path != defaultRunStorePath {
		t.Fatalf("expected default path %q, got %q", defaultRunStorePath, path)
	}
}

func TestRunStorePath_UsesEnvOverride(t *testing.T) {
	path := runStorePath(func(string) string { return "/var/lib/spawner/runstore.json" })
	if path != "/var/lib/spawner/runstore.json" {
		t.Fatalf("expected env override path, got %q", path)
	}
}

func TestSampleInput_BuildsExpectedRunSpec(t *testing.T) {
	in := sampleInput()

	rs, ok := in.Req.(*api.RunSpec)
	if !ok {
		t.Fatalf("expected *api.RunSpec, got %T", in.Req)
	}
	if rs.RunID != "run-001" {
		t.Fatalf("unexpected run id: %q", rs.RunID)
	}
	if rs.ImageRef == "" || len(rs.Mounts) != 2 {
		t.Fatalf("expected populated sample input, got image=%q mounts=%d", rs.ImageRef, len(rs.Mounts))
	}
	if rs.SpecVersion != 1 {
		t.Fatalf("expected spec version 1, got %d", rs.SpecVersion)
	}
	if rs.Annotations["example.com/pipeline"] != "genomics" {
		t.Fatalf("unexpected annotation set: %+v", rs.Annotations)
	}
	if rs.CorrelationID != "sample-HG001" {
		t.Fatalf("unexpected correlation id: %q", rs.CorrelationID)
	}
	if rs.Cleanup.TTLSecondsAfterFinished != 900 {
		t.Fatalf("unexpected cleanup ttl: %d", rs.Cleanup.TTLSecondsAfterFinished)
	}
	if in.Meta.RPC != "RunE" || in.Meta.TenantID != "teamA" {
		t.Fatalf("unexpected meta: %+v", in.Meta)
	}
}

func TestRunRule_BuildsRunCommand(t *testing.T) {
	rule := runRule()
	in := sampleInput()

	if !rule.Match(in) {
		t.Fatal("expected rule to match RunE input")
	}
	if key := rule.SpawnKeyFn(in); key != "teamA:run-001" {
		t.Fatalf("unexpected spawn key: %q", key)
	}

	cmd, err := rule.BuildCmd(in)
	if err != nil {
		t.Fatalf("BuildCmd: %v", err)
	}
	if cmd.Kind != api.CmdRun {
		t.Fatalf("expected CmdRun, got %q", cmd.Kind)
	}
	if cmd.Run == nil || cmd.Run.RunID != "run-001" {
		t.Fatalf("expected run payload to be preserved: %+v", cmd.Run)
	}
	if cmd.Policy.Timeout != 5*time.Minute {
		t.Fatalf("expected default timeout 5m, got %s", cmd.Policy.Timeout)
	}
}

func TestLogBootstrap_LogsOnlyWhenRecoveredRunsExist(t *testing.T) {
	var buf bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)

	logBootstrap(nil)
	if buf.Len() != 0 {
		t.Fatalf("expected no log output for empty recovery set, got %q", buf.String())
	}

	logBootstrap([]dispatcher.RecoverableRun{{Envelope: api.RunEnvelope{Identity: api.RunIdentity{LogicalRunID: "run-1"}}}})
	if !strings.Contains(buf.String(), "pending re-dispatch") {
		t.Fatalf("expected bootstrap log output, got %q", buf.String())
	}
}
