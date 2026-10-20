//go:build chaos || soak

package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/tests/harness"
)

const processHelperEnv = "LEADERELECTION_PROCESS_HELPER"

// Invoked by TestMain before touching Toxiproxy. This is an actual independent
// elector process: SIGKILL runs no cleanup, SIGSTOP freezes all its goroutines.
func runElectionProcess() int {
	backend := harness.PostgresBackend
	if os.Getenv("LE_PROCESS_BACKEND") == "mysql" {
		backend = harness.MySQLBackend
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	store, err := backend.Open(ctx, os.Getenv("LE_PROCESS_DSN"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer store.Close()
	elector, err := leaderelection.New(leaderelection.Config{
		Store: store, Name: os.Getenv("LE_PROCESS_ELECTION"), Identity: os.Getenv("LE_PROCESS_ID"),
		LeaseDuration: harness.LeaseDuration, RenewInterval: harness.RenewInterval,
		RetryPeriod: harness.RetryPeriod, OperationTimeout: harness.OperationTimeout, SafetyMargin: harness.SafetyMargin,
		Callbacks: leaderelection.Callbacks{OnStartedLeading: func(ctx context.Context, _ int64) { <-ctx.Done() }},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	done := make(chan error, 1)
	go func() { done <- elector.Run(ctx) }()
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		status := elector.Status()
		fence, valid := elector.Fence()
		if err := encoder.Encode(processStatus{
			IsLeader: status.IsLeader, Fence: status.Fence,
			DirectLeader: elector.IsLeader(), DirectFence: fence, FenceValid: valid,
		}); err != nil {
			return 1
		}
	}
	cancel()
	if err := <-done; err != nil {
		return 1
	}
	return 0
}

type processStatus struct {
	IsLeader     bool
	Fence        int64
	DirectLeader bool
	DirectFence  int64
	FenceValid   bool
}

type electionProcess struct {
	cmd     *exec.Cmd
	request *bufio.Writer
	replies chan processStatus
}

func startElectionProcess(t *testing.T, backend *harness.Backend, dsn, election, id string) *electionProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable)
	cmd.Env = append(os.Environ(), processHelperEnv+"=1", "LE_PROCESS_BACKEND="+backend.Name,
		"LE_PROCESS_DSN="+dsn, "LE_PROCESS_ELECTION="+election, "LE_PROCESS_ID="+id)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &electionProcess{cmd: cmd, request: bufio.NewWriter(stdin), replies: make(chan processStatus, 1)}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer close(p.replies)
		decoder := json.NewDecoder(stdout)
		for {
			var status processStatus
			if err := decoder.Decode(&status); err != nil {
				return
			}
			p.replies <- status
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = stdin.Close()
		<-readDone
	})
	return p
}

func (p *electionProcess) status(t *testing.T) processStatus {
	t.Helper()
	if _, err := p.request.WriteString("status\n"); err != nil {
		t.Fatal(err)
	}
	if err := p.request.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case status, ok := <-p.replies:
		if !ok {
			t.Fatal("elector process exited before reporting its status")
		}
		return status
	case <-time.After(10 * time.Second):
		t.Fatal("elector process stopped responding")
	}
	return processStatus{}
}

func awaitProcessLeader(t *testing.T, p *electionProcess, within time.Duration) processStatus {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if status := p.status(t); status.IsLeader {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("process did not acquire leadership")
	return processStatus{}
}

// Unlike Cluster.Kill these cases kill or freeze the entire runtime. A frozen
// process cannot run its watchdog until resumed, so public status and fencing
// must reject the old term before any watchdog has had a chance to run.
func TestRealProcessFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX signals")
	}
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			for _, fault := range []string{"kill", "freeze"} {
				t.Run(up.Name+"/"+fault, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					store, err := backend.Open(ctx, fmt.Sprintf(backend.DSNTemplate, backend.Admin))
					if err != nil {
						t.Fatal(err)
					}
					defer store.Close()
					if err := store.EnsureSchema(ctx); err != nil {
						t.Fatal(err)
					}
					election := "process-" + strconv.FormatInt(time.Now().UnixNano(), 10)
					tp := harness.NewToxiproxy(t, harness.ToxiproxyURL())
					port := ports(t, 2)
					firstProxy := tp.CreateProxy(t, election+"-first", port, up.Addr)
					first := startElectionProcess(t, backend, fmt.Sprintf(backend.DSNTemplate, firstProxy.Listen), election, "first-"+election)
					initial := awaitProcessLeader(t, first, 20*time.Second)
					signal := os.Signal(syscall.SIGKILL)
					if fault == "freeze" {
						signal = syscall.SIGSTOP
					}
					if err := first.cmd.Process.Signal(signal); err != nil {
						t.Fatal(err)
					}
					secondProxy := tp.CreateProxy(t, election+"-second", port+1, up.Addr)
					second := startElectionProcess(t, backend, fmt.Sprintf(backend.DSNTemplate, secondProxy.Listen), election, "second-"+election)
					successor := awaitProcessLeader(t, second, 20*time.Second)
					if successor.Fence <= initial.Fence {
						t.Fatalf("fence did not advance: %d -> %d", initial.Fence, successor.Fence)
					}
					if fault == "freeze" {
						if err := first.cmd.Process.Signal(syscall.SIGCONT); err != nil {
							t.Fatal(err)
						}
						deadline := time.Now().Add(2 * harness.LeaseDuration)
						for time.Now().Before(deadline) {
							status := first.status(t)
							if status.IsLeader || status.Fence != 0 || status.DirectLeader || status.FenceValid || status.DirectFence != 0 {
								t.Fatalf("resumed process exposed stale leadership: %+v", status)
							}
							if !second.status(t).IsLeader {
								t.Fatal("successor lost healthy leadership")
							}
							time.Sleep(10 * time.Millisecond)
						}
					}
				})
			}
		}
	}
}
