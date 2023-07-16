package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/masonictemple4/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// implement gRPC's resolver.Builder and resolver.Resolver interfaces
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn // user's client connection
	resolverConn  *grpc.ClientConn    // the resolver's own client connection to the server so it can call GetServers()
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// This is a great way to not only assert that our type satisfies an
// interface, but also to document what interfaces are being implemented
// on this type.
// If Resolver doesn't implement all methods of the interface, in this
// case resolver.Builder, this line will cause a compile-time error.
var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint(), dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	// register the resolver with grpc so when it finds a target with a
	// matching scheme it can use it.
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

// resolves the target, discover servers, and update the client
// connection with the servers.
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve target",
			zap.Error(err),
		)
		return
	}

	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
