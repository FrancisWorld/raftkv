// Command raftctl is a command-line client for a raftkv cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/FrancisWorld/raftkv/client"
)

const usage = `usage: raftctl [--endpoints url,url,...] [--timeout 10s] <command> [args]

commands:
  get <key>                      print a key's value (exit 1 if missing)
  put <key> <value>              set a key
  delete <key>                   remove a key
  cas <key> <expected> <value>   set a key only if it currently equals <expected>
  create <key> <value>           set a key only if it does not exist
  status                         show every endpoint's role, term and indexes
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoints := fs.String("endpoints", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083", "comma-separated node URLs")
	timeout := fs.Duration("timeout", 10*time.Second, "overall timeout for the command")
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return 2
	}
	eps := strings.Split(*endpoints, ",")
	c, err := client.New(eps)
	if err != nil {
		fmt.Fprintln(stderr, "raftctl:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	code, err := dispatch(ctx, c, eps, rest, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "raftctl:", err)
	}
	return code
}

var errUsage = errors.New("wrong number of arguments; run raftctl with no arguments for help")

func dispatch(ctx context.Context, c *client.Client, eps, args []string, out io.Writer) (int, error) {
	cmd, args := args[0], args[1:]
	need := map[string]int{"get": 1, "put": 2, "delete": 1, "cas": 3, "create": 2, "status": 0}
	n, ok := need[cmd]
	if !ok {
		return 2, fmt.Errorf("unknown command %q", cmd)
	}
	if len(args) != n {
		return 2, errUsage
	}
	switch cmd {
	case "get":
		v, found, err := c.Get(ctx, args[0])
		if err != nil {
			return 1, err
		}
		if !found {
			return 1, fmt.Errorf("%s: not found", args[0])
		}
		fmt.Fprintln(out, v)
	case "put":
		return report(out)(c.Put(ctx, args[0], args[1]))
	case "delete":
		return report(out)(c.Delete(ctx, args[0]))
	case "cas":
		return report(out)(c.CompareAndSwap(ctx, args[0], args[1], args[2]))
	case "create":
		return report(out)(c.CreateIfAbsent(ctx, args[0], args[1]))
	case "status":
		return status(ctx, c, eps, out)
	}
	return 0, nil
}

// report prints a write result; a refused conditional write exits non-zero.
func report(out io.Writer) func(client.Result, error) (int, error) {
	return func(r client.Result, err error) (int, error) {
		if err != nil {
			return 1, err
		}
		if !r.Applied {
			fmt.Fprintf(out, "not applied (current value: %q)\n", r.Prev)
			return 1, nil
		}
		if r.PrevFound {
			fmt.Fprintf(out, "OK (previous value: %q)\n", r.Prev)
		} else {
			fmt.Fprintln(out, "OK")
		}
		return 0, nil
	}
}

func status(ctx context.Context, c *client.Client, eps []string, out io.Writer) (int, error) {
	code := 0
	fmt.Fprintf(out, "%-26s %-9s %5s %7s %7s %8s %9s %5s\n", "ENDPOINT", "ROLE", "TERM", "LEADER", "COMMIT", "APPLIED", "SNAPSHOT", "KEYS")
	for _, ep := range eps {
		st, err := c.Status(ctx, ep)
		if err != nil {
			fmt.Fprintf(out, "%-26s unreachable (%v)\n", ep, err)
			code = 1
			continue
		}
		fmt.Fprintf(out, "%-26s %-9s %5d %7d %7d %8d %9d %5d\n", ep, st.Role, st.Term, st.Leader, st.CommitIndex, st.AppliedIndex, st.SnapshotIndex, st.Keys)
	}
	return code, nil
}
