package gojuggle

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// Config contains configuration options for the Juggler
type Config struct {
	EtcdEndPoints []string
	EtcdKeyPrefix string
	EtcdIndex     string
	EtcdTTL       time.Duration
	Measurer      Measurer
	Throttler     Throttler
}

// Measurer provides the local metric to monitor. A lower metric means lower
// load, and this means that this server should be able to handle more load. On
// the contrary a higher metric means a higher load, and this in turn means that
// the load should be lower.
type Measurer interface {
	Measure() (float64, error)
}

// Throttler receives a percentage in the range [0,1], indicating how much to
// throttle processing: 0 means no throttling (i.e. process at full speed), 1
// means full throttling (i.e. don't process incoming data)
type Throttler interface {
	Throttle(percentage float64)
}

// Juggler allows a distributed system to broadcast a local "load" metric to all
// other nodes, and to retrieve a measure of how much this node is overloaded
// compared to other nodes. This can be used in various ways, e.g. to throttle
// the incoming workload.
type Juggler struct {
	conf         Config
	etcd         *etcd.Client
	rCh          chan *etcd.Response
	sCh          chan bool
	eCh          chan error
	etcdLocalKey string
}

// NewJuggler creates and starts a new Juggler
func NewJuggler(c Config) (*Juggler, chan error) {
	c.EtcdKeyPrefix = strings.Trim(c.EtcdKeyPrefix, "/")
	if c.EtcdIndex == "" {
		c.EtcdIndex, _ = os.Hostname()
	}
	if c.EtcdTTL <= 0 {
		c.EtcdTTL = 30 * time.Second
	}

	j := &Juggler{
		conf:         c,
		etcd:         etcd.NewClient(c.EtcdEndPoints),
		rCh:          make(chan *etcd.Response),
		sCh:          make(chan bool),
		eCh:          make(chan error),
		etcdLocalKey: "/" + c.EtcdKeyPrefix + "/" + c.EtcdIndex,
	}
	if j.conf.Throttler != nil {
		go func() {
			_, err := j.etcd.Watch(j.conf.EtcdKeyPrefix, 0, true, j.rCh, j.sCh)
			j.eCh <- err
		}()
		go j.receiveMetrics()
	}
	if j.conf.Measurer != nil {
		go j.sendMetrics()
	}
	return j, j.eCh
}

// Stop stops a Juggler. It is illegal to do any further operations on a Juggler
// after it has been stopped.
func (j *Juggler) Stop() {
	close(j.sCh)
	j.etcd.Close()
}

// SendMetric broadcasts the current metric value (this method can be used
// directly when not using a Measurer, or also in cases when a new metric
// readout is available and we want to broadcast it ASAP). Note that this
// requires updating a key on etcd (and this will cause every node where a
// Throttler is running to pull the state of all nodes from etcd).
func (j *Juggler) SendMetric(metric float64) error {
	value := strconv.FormatFloat(metric, 'G', -1, 64)
	ttl := uint64(j.conf.EtcdTTL.Seconds())
	_, err := j.etcd.Set(j.etcdLocalKey, value, ttl)
	return err
}

// GetThrottle returns the current throttling percentage, i.e. a measure of how
// much this node is overloaded compared to the other nodes (this can be used
// directly when not using a Throttler, but also in case an immediate readout is
// required). Note that this requires fetching the state of all nodes from etcd.
func (j *Juggler) GetThrottle() (percentage float64, err error) {
	_, percentage, err = j.getMetrics(nil)
	return
}

func (j *Juggler) getMetrics(upd *etcd.Response) (map[string]float64, float64, error) {
	res, err := j.etcd.Get(j.conf.EtcdKeyPrefix, false, true)
	if err != nil {
		return nil, 0, err
	}
	if res.Node.Dir == false {
		return nil, 0, fmt.Errorf("Expecting directory")
	}

	metrics := make(map[string]float64)
	for _, metric := range res.Node.Nodes {
		value, err := strconv.ParseFloat(metric.Value, 64)
		if err == nil {
			metrics[metric.Key] = value
		}
	}

	if upd != nil {
		if upd.Action == "delete" || upd.Action == "expire" {
			delete(metrics, upd.Node.Key)
		} else {
			value, err := strconv.ParseFloat(upd.Node.Value, 64)
			if err == nil {
				metrics[upd.Node.Key] = value
			}
		}
	}

	var local, sum, max, avg, throttle float64
	for _, value := range metrics {
		sum += value
		if value > max {
			max = value
		}
	}
	if len(metrics) > 0 {
		avg = sum / float64(len(metrics))
		local = metrics[j.etcdLocalKey]
		if local > avg {
			throttle = (local - avg) / (max - avg)
		}
	}

	return metrics, throttle, nil
}

func (j *Juggler) sendMetrics() {
	tickTime := j.conf.EtcdTTL / 3
	log.Printf("Starting measurer, tick time: %.1fs", tickTime.Seconds())
	defer log.Print("Measurer ended")
	tick := time.Tick(tickTime)
	for {
		select {
		case <-tick:
			m, err := j.conf.Measurer.Measure()
			if err == nil {
				log.Printf("Measurer returned value %f", m)
				err = j.SendMetric(m)
				if err != nil {
					log.Printf("Failed sending metric: %v", err)
				}
			} else {
				log.Printf("Measurer failed: %v", err)
			}
		case <-j.sCh:
			log.Print("Stopping measurer")
			return
		}
	}
}

func (j *Juggler) receiveMetrics() {
	log.Print("Starting throttler")
	defer log.Print("Throttler ended")
	for {
		select {
		case res := <-j.rCh:
			_, throttle, err := j.getMetrics(res)
			if err == nil {
				log.Printf("Throtlling to %.0f%%", throttle*100)
				j.conf.Throttler.Throttle(throttle)
			} else {
				log.Printf("Throtlling failed: %v", err)
			}
		case <-j.sCh:
			log.Print("Stopping throttler")
			return
		}
	}
}
