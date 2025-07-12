package etcdplugin

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/coredhcp/coredhcp/handler"
	"github.com/coredhcp/coredhcp/plugins/allocators/bitmap"
	"github.com/go-viper/encoding/javaproperties"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

func setup(args0 ...string) (handler.Handler4, error) {
	args := strings.Join(args0, "\n")

	codecRegistry := viper.NewCodecRegistry()
	codec := &javaproperties.Codec{}
	codecRegistry.RegisterCodec("properties", codec)

	v := viper.NewWithOptions(
		viper.WithCodecRegistry(codecRegistry),
	)
	v.SetConfigType("properties")
	if err := v.ReadConfig(bytes.NewBuffer([]byte(args))); err != nil {
		return nil, fmt.Errorf("unable to read config: %w", err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to unmarshal config: %w", err)
	}

	log.Infof("%s", config)

	if config.Separator == "" {
		config.Separator = constDefaultSeparator
	}

	ctx := context.Background()

	client, err := NewClient(ctx, config)
	if err != nil {
		return nil, err
	}

	ipStart := net.ParseIP(config.Start)
	if ipStart.To4() == nil {
		return nil, fmt.Errorf("invalid IPv4 address: %v", config.Start)
	}
	ipEnd := net.ParseIP(config.End)
	if ipEnd.To4() == nil {
		return nil, fmt.Errorf("invalid IPv4 address: %v", config.End)
	}
	if binary.BigEndian.Uint32(ipStart.To4()) >= binary.BigEndian.Uint32(ipEnd.To4()) {
		return nil, errors.New("start of IP range has to be lower than the end of an IP range")
	}

	allocator, err := bitmap.NewIPv4Allocator(ipStart, ipEnd)
	if err != nil {
		return nil, fmt.Errorf("could not create an allocator: %w", err)
	}

	dns, err := NewDNS(config.DNSPrefix, config.DNSZone, config.Separator, config.DNSNames)
	if err != nil {
		return nil, fmt.Errorf("could not initialize DNS: %w", err)
	}

	grp, ctx := errgroup.WithContext(ctx)

	p := PluginState{
		config:    config,
		client:    client,
		allocator: allocator,
		dns:       dns,
		grp:       grp,
	}

	if err := p.bootstrapLeasableRange(ctx); err != nil {
		return nil, fmt.Errorf("unable to bootstrap leasable range: ", err)
	}

	grp.Go(func() error {
		log.Info("starting lease monitor")
		err := p.monitorLeases(ctx, 10*time.Second)
		return errors.Wrap(err, "could not monitor leases")
	})

	return p.Handler4, nil
}
