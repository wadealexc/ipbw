package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/multiformats/go-multiaddr"
)

type logOutput struct {
	Id       string
	Agent    string
	Protocol string
	Addrs    []string
	Protos   []string
}

type Crawler struct {
	host host.Host // Our libp2p node
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	mu sync.Mutex
	db *sql.DB

	ctx    context.Context
	cancel context.CancelFunc
	// Waits for a collection of goroutines to finish
	// Main goroutine (this file) calls wg.Add to set the number of goroutines to wait for
	// Then, each goroutine runs and calls Done when finished
	wg sync.WaitGroup
}

// Alias Crawler as notifee, which will implement network.Notifiee interface
type notifee Crawler

// Declare that alias'd Crawler implements network.Notifiee
var _ network.Notifiee = (*notifee)(nil)

func NewCrawler() *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		ctx:    ctx,
		cancel: cancel,
	}

	// c.initDB()
	c.initHost()
	return c
}

func (c *Crawler) initDB() {
	// init c.db
}

func (c *Crawler) initHost() {
	var err error
	// Create a new libp2p node
	c.host, err = libp2p.New(context.Background())
	if err != nil {
		panic(err)
	}

	// Create a DB for our DHT client
	c.ds, err = badger.NewDatastore("dht.db", nil)
	if err != nil {
		panic(err)
	}

	// Create DHT client
	c.dht = dht.NewDHTClient(c.ctx, c.host, c.ds)

	// Iterate over default bootstrap peers
	for _, a := range dht.DefaultBootstrapPeers {
		// Convert multiaddr to AddrInfo
		pi, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			panic(err)
		}

		// Connect to each bootstrap peer w/ 5 second timeout
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		if err = c.host.Connect(ctx, *pi); err != nil {
			fmt.Printf("skipping bootstrap peer: %s\n", pi.ID.Pretty())
		}

		cancel()
	}

	c.id = identify.NewIDService(c.host)
}

func (c *Crawler) close() {
	c.cancel()
	c.wg.Done()

	// if err := c.db.Close(); err != nil {
	// 	fmt.Printf("error while shutting down: %v\n", err)
	// }
	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
}

const WORKERS = 8

func (c *Crawler) start() {
	fmt.Println("starting crawler...")

	for i := 0; i < WORKERS; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.reporter()

	c.host.Network().Notify((*notifee)(c))
}

func (c *Crawler) worker() {
	fmt.Println("Worker started.")
Work:
	// Get a random peer ID
	id, err := test.RandPeerID()
	if err != nil {
		panic(err)
	}

	// fmt.Printf("looking for peer: %s\n", id)
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	_, _ = c.dht.FindPeer(ctx, id)
	cancel()
	goto Work
}

func (c *Crawler) reporter() {
	fmt.Println("Tried to report, but we don't have a DB!")
	// var (
	// 	count int
	// 	row   *sql.Row
	// 	err   error
	// )

	// for {
	// 	select {
	// 	case <-time.After(10 * time.Second):
	// 		row = c.db.QueryRow("SELECT COUNT(id) FROM peers")
	// 		if err = row.Scan(&count); err != nil {
	// 			panic(err)
	// 		}
	// 		fmt.Printf("--- found %d peers\n", count)
	// 	case <-c.ctx.Done():
	// 		return
	// 	}
	// }
}

func (n *notifee) Connected(net network.Network, conn network.Conn) {
	p := conn.RemotePeer()
	pstore := net.Peerstore()

	go func() {
		// Ask the IDService about the connection
		n.id.IdentifyConn(conn)
		<-n.id.IdentifyWait(conn)

		// Get the protocols supported by the peer
		protos, err := pstore.GetProtocols(p)
		if err != nil {
			panic(err)
		}

		// Get all known / valid addresses for the peer
		addrs := pstore.Addrs(p)

		// Get peer's agent version
		agent, err := pstore.Get(p, "AgentVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			agent = ""
		default:
			panic(err)
		}

		// Get peer's protocol version
		protocol, err := pstore.Get(p, "ProtocolVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			protocol = ""
		default:
			panic(err)
		}

		line := logOutput{
			Id:       p.Pretty(),
			Agent:    agent.(string),
			Protocol: protocol.(string),
			Addrs: func() (ret []string) {
				for _, a := range addrs {
					ret = append(ret, a.String())
				}
				return ret
			}(),
			Protos: protos,
		}

		fmt.Printf("Connected to peer: \n%+v\n", line)

		// if !*update {
		// 	var cnt int
		// 	row := statements.GetPeer.stmt.QueryRow(p.Pretty())
		// 	if err := row.Scan(&cnt); err != nil {
		// 		panic(err)
		// 	}
		// 	if cnt > 0 {
		// 		fmt.Println("(duplicate peer; skipping)")
		// 		return
		// 	}
		// }

		// n.mu.Lock()
		// defer n.mu.Unlock()

		// if _, err = statements.UpsertPeer.stmt.Exec(p.Pretty(), agent, protocol); err != nil {
		// 	panic(err)
		// }
		// if _, err = statements.DeleteAddrs.stmt.Exec(p.Pretty()); err != nil {
		// 	panic(err)
		// }
		// for _, addr := range addrs {
		// 	if _, err = statements.AddAddr.stmt.Exec(p.Pretty(), addr.String()); err != nil {
		// 		panic(err)
		// 	}
		// }
		// if _, err = statements.DeleteProtos.stmt.Exec(p.Pretty()); err != nil {
		// 	panic(err)
		// }
		// for _, proto := range protos {
		// 	if _, err = statements.AddProtos.stmt.Exec(p.Pretty(), proto); err != nil {
		// 		panic(err)
		// 	}
		// }
	}()
}

func (*notifee) Disconnected(_ network.Network, conn network.Conn) {
	fmt.Printf("Disconnected from peer: %s\n", conn.RemotePeer())
}

func (*notifee) Listen(_ network.Network, maddr multiaddr.Multiaddr) {
	fmt.Printf("Started listening on address: %s\n", maddr.String())
}

func (*notifee) ListenClose(_ network.Network, maddr multiaddr.Multiaddr) {
	fmt.Printf("Stopped listening on address: %s\n", maddr.String())
}

func (*notifee) OpenedStream(_ network.Network, st network.Stream) {
	// fmt.Printf("Opened stream with ID: %s\n", st.ID())
}

func (*notifee) ClosedStream(_ network.Network, st network.Stream) {
	// fmt.Printf("Closed stream with ID: %s\n", st.ID())
}

func main() {

	crawler := NewCrawler()

	crawler.start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	select {
	case <-ch:
		crawler.close()
		return
	case <-crawler.ctx.Done():
		return
	}

}
