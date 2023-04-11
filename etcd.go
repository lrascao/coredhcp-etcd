package etcdplugin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"
)

func NewClient(ctx context.Context, c Config) (*etcd.Client, error) {
	conf, err := etcdConfig(c)
	if err != nil {
		return nil, errors.WithMessage(err, "could not load etcd config")
	}

	client, err := etcd.New(conf)
	if err != nil {
		return nil, errors.Wrap(err, "could not create etcd client")
	}

	err = client.Sync(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform initial etcd endpoint sync")
	}

	go func() {
		for ctx.Err() == nil {
			func() {
				ctx, cancel := context.WithTimeout(ctx, time.Second*30)
				defer cancel()

				err := client.Sync(ctx)
				if err != nil {
					log.Error("failed to sync etcd endpoints: %v", err)
					// crash so systemd can restart it and hopefully recover
					panic(err)
				} else {
					log.Info("synced etcd endpoint list")
				}
			}()

			select {
			case <-time.After(time.Second * 60):
			case <-ctx.Done():
			}
		}
	}()

	return client, nil
}

func etcdConfig(c Config) (etcd.Config, error) {
	caCertPool := x509.NewCertPool()
	caCert, err := ioutil.ReadFile(c.CA)
	if err != nil {
		return etcd.Config{}, errors.Wrap(err, "could not load etcd CA")
	}
	caCertPool.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
	if err != nil {
		return etcd.Config{}, errors.Wrap(err, "could not load etcd client key pair")
	}
	certificates := []tls.Certificate{cert}

	return etcd.Config{
		Endpoints: c.Endpoints,
		TLS: &tls.Config{
			Certificates: certificates,
			RootCAs:      caCertPool,
		},
	}, nil
}
