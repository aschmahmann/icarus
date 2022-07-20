package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	cli "github.com/urfave/cli/v2"

	bsnet "github.com/ipfs/boxo/bitswap/network"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/labstack/gommon/log"
	"github.com/libp2p/go-libp2p"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func main() {
	app := cli.NewApp()

	app.Commands = []*cli.Command{
		{
			Name:        "serve",
			Usage:       "<car-path>",
			Description: "runs an ephemeral node that serves a CAR file",
			Action: func(ctx *cli.Context) error {
				carPath := ctx.Args().First()
				cbs, err := carbs.OpenReadOnly(carPath)
				if err != nil {
					return err
				}

				// Don't listen on quic-v1 since it's not supported by IPNI at the moment
				addrs := []string{
					"/ip4/0.0.0.0/tcp/0",
					"/ip4/0.0.0.0/udp/0/quic",
					"/ip6/::/tcp/0",
					"/ip6/::/udp/0/quic",
				}

				h, err := libp2p.New(libp2p.NATPortMap(), libp2p.ListenAddrStrings(addrs...))
				if err != nil {
					return err
				}

				// Wait for nap port mapping to set up
				time.Sleep(time.Second * 10)

				n := bsnet.NewFromIpfsHost(h, &routinghelpers.Null{})
				bssrv := bsserver.New(ctx.Context, n, cbs)
				n.Start(bssrv)

				// Only print the first three characters to keep golang example output happy.
				fmt.Printf("✓ Instantiated new libp2p host with peer ID: %s\n", h.ID().String())
				fmt.Printf("libp2p addresses: %v\n", h.Addrs())

				// Construct a new provider engine with given libp2p host that announces advertisements over
				// gossipsub and datatrasfer/graphsync.
				eng, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher), engine.WithDirectAnnounce("https://cid.contact/ingest/announce"))
				if err != nil {
					panic(err)
				}
				fmt.Println("✓ Instantiated provider engine")
				defer eng.Shutdown()

				ctxId := []byte(fmt.Sprintf("car-%d", rand.Uint64()))

				eng.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {
					if !bytes.Equal(ctxId, contextID) {
						return nil, fmt.Errorf("do not have the given context id")
					}
					idx := cbs.Index().(index.IterableIndex)
					mhCh := make(chan multihash.Multihash)
					go func() {
						defer close(mhCh)
						err := idx.ForEach(func(m multihash.Multihash, u uint64) error {
							mhCh <- m
							return nil
						})
						if err != nil {
							log.Error(err)
						}
					}()

					w := &mhiterwrapper{
						func() (multihash.Multihash, error) {
							nextMh, more := <-mhCh
							if !more {
								return nil, io.EOF
							}
							return nextMh, nil
						},
					}

					return w, nil
				})

				// Start the engine
				if err = eng.Start(ctx.Context); err != nil {
					return err
				}

				// Note that this example publishes an ad with bitswap metadata as an example.
				// But it does not instantiate a bitswap server to serve retrievals.
				md := metadata.Default.New(metadata.Bitswap{})
				adCid, err := eng.NotifyPut(ctx.Context, nil, ctxId, md)
				if err != nil {
					return err
				}
				fmt.Printf("ad cid: %s\n", adCid)
				<-ctx.Done()

				return nil
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

type mhiterwrapper struct {
	fn func() (multihash.Multihash, error)
}

func (m *mhiterwrapper) Next() (multihash.Multihash, error) {
	return m.fn()
}

var _ provider.MultihashIterator = (*mhiterwrapper)(nil)
