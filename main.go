// Program sqs-worker-pool starts multiple worker processes for each matching
// SQS queue depending on queue depth.
//
// Its main purpose is maintaining pool of workers for multiple queues with
// uneven load, when constantly running multiple workers may be a waste of
// resources.
//
// Upon start it fetches list of all SQS queues matching given conditions (see
// -include and -exclude flags), then starts worker pool for each matched queue.
// Each worker pool polls queue depth once a minute and calculates target number
// of workers. Target number of workers in a pool is calculated as queue depth
// / worker-load, but capped to max-workers. For non-empty queues, target number
// is always in [1, max-workers] range. Pool does not terminate workers by
// itself, workers should terminate if they see that queue became idle.
//
// Program starts a single process as a worker for each queue, this process can
// be used as a dispatcher to run different processes depending on queue name.
// Worker process is called with queue name as its first positional argument and
// SQS queue url as a second positional argument. These values are also passed
// via environment as NAME and URL variables respectively.
//
// Upon receiving INT or TERM signals, program tries gracefully shutting down
// all running workers by sending them TERM first, and KILL 3 seconds later.
//
// sqs-worker-pool is built using AWS SDK, so it looks up required credentials
// in a usual way: via local confgiuration, environment variables, IAM role.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/artyom/autoflags"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/sync/errgroup"
)

func main() {
	args := runArgs{
		MaxWorkers: 10,
		WorkerLoad: 1000,
	}
	autoflags.Parse(&args)
	if os.Getenv("AWS_EXECUTION_ENV") != "" {
		log.SetFlags(0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		defer signal.Stop(ch)
		log.Printf("%s, shutting down", <-ch)
		cancel()
	}()
	if err := run(ctx, args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type runArgs struct {
	Executable string `flag:"worker,path to worker executable"`
	Include    string `flag:"include,regex of queue names to include, empty matches all"`
	Exclude    string `flag:"exclude,regex of queue names to exclude, empty matches none"`
	MaxWorkers int    `flag:"max-workers,maximum number of workers to run per queue"`
	WorkerLoad int    `flag:"worker-load,number of jobs single worker can process over poll cycle"`
	ListOnly   bool   `flag:"list,only print matching queue urls to stdout and exit"`
	Verbose    bool   `flag:"verbose,don't suppress workers' stdout/stderr"`
}

func (a *runArgs) check() error {
	if a.MaxWorkers < 1 {
		return errors.New("max-workers must be positive")
	}
	if a.WorkerLoad < 1 {
		return errors.New("worker-load must be positive")
	}
	if a.Executable == "" {
		return errors.New("worker must be set")
	}
	if _, err := exec.LookPath(a.Executable); err != nil {
		return err
	}
	return nil
}

func run(ctx context.Context, args runArgs) error {
	if err := args.check(); err != nil {
		return err
	}
	var prefix string
	var reInclude, reExclude *regexp.Regexp
	var err error
	if args.Include != "" {
		if reInclude, err = regexp.Compile(args.Include); err != nil {
			return err
		}
		prefix, _ = reInclude.LiteralPrefix()
	}
	if args.Exclude != "" {
		if reExclude, err = regexp.Compile(args.Exclude); err != nil {
			return err
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	if out, err := imds.NewFromConfig(cfg).GetRegion(ctx, nil); err == nil {
		cfg.Region = out.Region
	}
	svc := sqs.NewFromConfig(cfg)
	queues, err := queueList(ctx, svc, prefix)
	if err != nil {
		return err
	}
	queues = filterQueues(queues, reInclude, reExclude)
	if args.ListOnly {
		for _, u := range queues {
			fmt.Println(u)
		}
		return nil
	}
	if len(queues) == 0 {
		return errors.New("empty queue list")
	}
	var stderr atomic.Value // []byte of stderr of the last failed command
	var g errgroup.Group
	g.Go(func() error {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGUSR1)
		defer signal.Stop(ch)
		const format = "last non-empty stderr of a failed command:\n%s"
		for {
			select {
			case <-ch:
				if val := stderr.Load(); val != nil {
					log.Printf(format, val.([]byte))
				}
			case <-ctx.Done():
				return nil
			}
		}
	})
	for _, url := range queues {
		p := newPool(args.Executable, url)
		p.verbose = args.Verbose
		p.saveStderr = func(b []byte) { stderr.Store(b) }
		p.logf("worker pool for %q", url)
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Duration(rand.Intn(2000)) * time.Millisecond):
			}
			return p.loop(ctx, svc, time.Minute, args.MaxWorkers, args.WorkerLoad)
		})
	}
	return g.Wait()
}

// queueList returns urls of every SQS queue which name starts with prefix
func queueList(ctx context.Context, svc *sqs.Client, prefix string) ([]string, error) {
	input := &sqs.ListQueuesInput{}
	if prefix != "" {
		input.QueueNamePrefix = &prefix
	}
	var queues []string
	p := sqs.NewListQueuesPaginator(svc, input)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		queues = append(queues, page.QueueUrls...)
	}
	return queues, nil
}

func newPool(bin, url string) *workerPool {
	p := &workerPool{bin: bin, url: url,
		procs: make(map[uint64]*exec.Cmd)}
	if i := strings.LastIndexByte(p.url, '/'); i >= 0 {
		p.name = p.url[i+1:]
	}
	return p
}

// workerPool tracks workers for a single queue
type workerPool struct {
	bin  string // binary path
	url  string // SQS queue url
	name string // SQS queue name (url part after final /)

	nextpid uint64 // internal pid counter, incremented with atomics

	// called for command's stderr, if command exits with error, has
	// non-empty stderr and this function is not nil
	saveStderr func([]byte)

	verbose bool // whether to connect workers' stdout/stderr to os.Stdout/Stderr

	mu    sync.Mutex
	procs map[uint64]*exec.Cmd // keyed by internal pid
}

// loop blocks until ctx is canceled, checking number of jobs in queue every
// d and starting worker processes according to maxWorkers and workerLoad. It
// does not terminate workers when queue is drained, it's expected that workers
// will shutdown when they got no job for a while. Target number of workers in
// a pool is calculated as queueSize / workerLoad, but capped to maxWorkers. For
// non-empty queue, target number is always in [1, maxWorkers] range.
//
// It returns either when ctx is canceled or it finds out that queue does not
// exist. It terminates all workers before returning.
func (p *workerPool) loop(ctx context.Context, svc *sqs.Client, d time.Duration, maxWorkers, workerLoad int) error {
	defer p.terminate(context.Background(), 3*time.Second)
	stub := make(chan struct{})
	close(stub) // to unblock first iteration early
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-stub: // initial iteration
			stub = nil // nil chan blocks forever
		}
		size, err := qSize(ctx, svc, p.url)
		if err != nil {
			if isNotExist(err) {
				return err
			}
			p.logf("%q queue size error: %v", p.name, err)
			continue
		}
		if size <= 0 {
			continue
		}
		cnt := size / workerLoad
		switch {
		case cnt > maxWorkers:
			cnt = maxWorkers
		case cnt == 0:
			cnt = 1
		}
		need := cnt - p.size()
		if need <= 0 {
			continue
		}
		s := "" // plural -s suffix
		if need > 1 {
			s = "s"
		}
		p.logf("%q queue size is ≈ %d, starting %d worker%s", p.name, size, need, s)
		for i := 0; i < need; i++ {
			if err := p.start(); err != nil {
				p.logf("error starting %q queue worker: %v", p.name, err)
				break
			}
		}
	}
}

// size returns number of currently registered processes in a pool. This will
// almost always be number of running processes.
func (p *workerPool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.procs)
}

func (p *workerPool) logf(format string, v ...interface{}) {
	log.Printf(format, v...) // TODO
}

// terminate gracefully terminates all processes in a pool by first sending
// SIGTERM, then sending SIGKILL either when killDelay passes or context is
// canceled.
func (p *workerPool) terminate(ctx context.Context, killDelay time.Duration) {
	if p.signal(syscall.SIGTERM) == 0 {
		return
	}
	if killDelay <= 0 {
		p.signal(syscall.SIGKILL)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, killDelay)
	<-ctx.Done()
	cancel()
	p.signal(syscall.SIGKILL)
}

// signal sends given signal to every running process in a pool, returning
// number of running processes
func (p *workerPool) signal(s os.Signal) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var cnt int
	for _, cmd := range p.procs {
		if cmd.Process == nil {
			continue
		}
		cmd.Process.Signal(s)
		cnt++
	}
	return cnt
}

// start starts new worker process, adding it to the pool. When process
// finishes, it is automatically removed from the pool.
//
// Process is started with SQS queue name as its first argument and queue url as
// a second argument; they also passed via environment as "NAME" and "URL".
func (p *workerPool) start() error {
	cmd := exec.Command(p.bin, p.name, p.url)
	cmd.Env = append(os.Environ(), "NAME="+p.name, "URL="+p.url)
	cmd.SysProcAttr = procAttr()
	if p.verbose {
		cmd.Stderr, cmd.Stdout = os.Stderr, os.Stdout
	}
	if !p.verbose && p.saveStderr != nil {
		cmd.Stderr = &prefixSuffixSaver{N: 32 << 10}
	}
	begin := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}
	pid := atomic.AddUint64(&p.nextpid, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.procs[pid] = cmd
	go func() {
		if err := cmd.Wait(); err != nil {
			p.logf("queue %q worker exit %v since start: %v", p.name,
				time.Since(begin).Truncate(time.Millisecond), err)
			if s, ok := cmd.Stderr.(*prefixSuffixSaver); ok &&
				s.prefix != nil && p.saveStderr != nil {
				p.saveStderr(s.Bytes())
			}
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.procs, pid)
	}()
	return nil
}

// filterQueues filters SQS queue urls by matching their names (final url
// element) against reInclude and reExclude. If reInclude is not nil, only
// matching urls pass filter, if reExclude is not nil, only those that don't
// match it pass filter. Matching is done with Regexp.MatchString method.
func filterQueues(queues []string, reInclude, reExclude *regexp.Regexp) []string {
	if reInclude == nil && reExclude == nil {
		return queues
	}
	var out []string
	for _, q := range queues {
		var name string
		if i := strings.LastIndexByte(q, '/'); i >= 0 {
			name = q[i+1:]
		}
		if name == "" {
			continue
		}
		if reInclude != nil && !reInclude.MatchString(name) {
			continue
		}
		if reExclude != nil && reExclude.MatchString(name) {
			continue
		}
		out = append(out, q)
	}
	return out
}

func qSize(ctx context.Context, svc *sqs.Client, url string) (int, error) {
	input := &sqs.GetQueueAttributesInput{
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
		QueueUrl: &url,
	}
	res, err := svc.GetQueueAttributes(ctx, input)
	if err != nil {
		return 0, err
	}
	val, ok := res.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	if !ok {
		return 0, errors.New("no required attribute in GetQueueAttributes response")
	}
	return strconv.Atoi(val)
}

// isNotExist returns true if error signals that queue does not exist
func isNotExist(err error) bool {
	var errNotExists *types.QueueDoesNotExist
	return errors.As(err, &errNotExists)
}
