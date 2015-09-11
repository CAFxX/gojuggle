package throttlers

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
)

type UDPThrottler struct {
	s        *net.UDPConn
	throttle float64
	override float64
}

func NewUDPThrottlerFromConn(listener *net.UDPConn) *UDPThrottler {
	return &UDPThrottler{s: listener, override: math.NaN()}
}

func NewUDPThrottler(port int) (*UDPThrottler, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("Unable to resolve listening address: %v", err)
	}
	udpConn, err := net.ListenUDP(udpAddr.Network(), udpAddr)
	if err != nil {
		return nil, fmt.Errorf("Unable to listen for UDP packets on addr %v: %v", udpAddr, err)
	}
	return NewUDPThrottlerFromConn(udpConn), nil
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
