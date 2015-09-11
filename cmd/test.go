package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cafxx/gojuggle"
	"github.com/cafxx/gojuggle/measurers"
	"github.com/cafxx/gojuggle/throttlers"
)

var port = flag.Int("port", 0, "UDP port to listen on")
var path = flag.String("path", "", "Filesystem path to monitor for usage")
var etcdEP = flag.String("etcd", "", "etcd endpoints to connect to")
var etcdTTL = flag.Int("etcd-ttl", 60, "TTL for etcd records (seconds)")
var etcdKeyPrefix = flag.String("etcd-key", "", "Prefix for etcd keys")
var etcdIndex = flag.String("etcd-id", "", "Unique index for this node")

func main() {
	if *port <= 0 || *port >= 1<<16 {
		log.Fatalf("Illegal UDP port number %d (--port)", *port)
	}
	if *path == "" {
		log.Fatal("No path specified (--path)")
	}
	fileinfo, err := os.Stat(*path)
	if err == nil {
		log.Fatalf("Specified path \"%s\" is invalid (--path): %v", *path, err)
	}
	if !fileinfo.IsDir() {
		log.Fatalf("Specified path \"%s\" is not a directory (--path)", *path)
	}
	var etcd []string
	if *etcdEP != "" {
		etcd = strings.Split(*etcdEP, ",")
	}

	throttler, err := throttlers.NewUDPThrottler(*port)
	if err != nil {
		log.Fatalf("Error in UDPThrottler: %v", err)
	}

	go throttler.Run()
	log.Printf("UDPThrottler listening on port %d", *port)

	measurer := measurers.NewDiskSpaceMeasurer(*path)
	log.Printf("DiskSpaceMeasurer monitoring %s", *path)

	j, eCh := gojuggle.NewJuggler(gojuggle.Config{
		EtcdEndPoints: etcd,
		EtcdKeyPrefix: *etcdKeyPrefix,
		EtcdIndex:     *etcdIndex,
		EtcdTTL:       time.Duration(*etcdTTL) * time.Second,
		Measurer:      measurer,
		Throttler:     throttler,
	})

	select {
	case err, ok := <-eCh:
		if ok && err != nil {
			log.Printf("Juggler error: %v", err)
		}
		break
	case <-time.After(1000 * time.Second):
		break
	}
	log.Print("Shutting down")
	j.Stop()
	<-time.After(1 * time.Second)
}
