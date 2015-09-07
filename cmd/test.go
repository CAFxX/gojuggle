package main

import (
	"log"
	"math"
	"math/rand"
	"net"
	"syscall"
	"time"

	"github.com/cafxx/gojuggle"
)

type DiskSpaceMeasurer struct {
	path string
}

func (m *DiskSpaceMeasurer) Measure() (float64, error) {
	var res syscall.Statfs_t
	if err := syscall.Statfs(m.path, &res); err != nil {
		return 0, err
	}
	return float64(res.Blocks-res.Bavail) * float64(res.Bsize), nil
}

type UDPThrottler struct {
	s        *net.UDPConn
	throttle float64
	override float64
}

func NewUDPThrottler(listener *net.UDPConn) *UDPThrottler {
	return &UDPThrottler{s: listener, override: math.NaN()}
}

func (u *UDPThrottler) Throttle(percentage float64) {
	u.throttle = percentage
}

func (u *UDPThrottler) Override(percentage float64) {
	if percentage < 0 || percentage >= 1 {
		u.override = math.NaN()
	} else {
		u.override = percentage
	}
}

func (u *UDPThrottler) Run() {
	buf := make([]byte, 1<<16)
	for {
		_, addr, err := u.s.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading UDP packet from addr %v: %v", addr, err)
			continue
		}

		throttle := u.throttle
		if !math.IsNaN(u.override) {
			throttle = u.override
		}

		if rand.Float64() < throttle {
			log.Printf("Ping from addr %v (%.1f%%): throttled", addr, u.throttle*100.0)
		} else if _, err := u.s.WriteToUDP([]byte("OK"), addr); err != nil {
			log.Printf("Ping from addr %v (%.1f%%): reply failed", addr, u.throttle*100.0)
		} else {
			log.Printf("Ping from addr %v (%.1f%%): replied", addr, u.throttle*100.0)
		}
	}
}

func main() {
	udpAddr, err := net.ResolveUDPAddr("udp", ":5302")
	if err != nil {
		log.Fatalf("Unable to resolve listening address: %v", err)
	}
	udpConn, err := net.ListenUDP(udpAddr.Network(), udpAddr)
	if err != nil {
		log.Fatalf("Unable to listen for UDP packets on addr %v: %v", udpAddr, err)
	}
	throttler := NewUDPThrottler(udpConn)
	go throttler.Run()
	log.Printf("UDPThrottler listening on addr %v", udpAddr)

	measurer := &DiskSpaceMeasurer{path: "/Users/carlo.ferraris"}

	j, eCh := gojuggle.NewJuggler(gojuggle.Config{
		EtcdEndPoints: nil,
		EtcdKeyPrefix: "test/key",
		EtcdTTL:       15 * time.Second,
		Measurer:      measurer,
		Throttler:     throttler,
	})
	select {
	case <-eCh:
		break
	case <-time.After(1000 * time.Second):
		break
	}
	j.Stop()
	<-time.After(1 * time.Second)
}
