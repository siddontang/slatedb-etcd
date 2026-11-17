// Build the pinned etcd server with its original command entry point.
package main

import (
	"go.etcd.io/etcd/server/v3/etcdmain"
	"os"
)

func main() { etcdmain.Main(os.Args) }
