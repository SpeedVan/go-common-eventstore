package eventstore

import (
	"log"
	"net"
	"net/url"
	"strings"

	"github.com/jdextraze/go-gesclient"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/tasks"
)

// Client todo
type Client struct {
	EsClient client.Connection
	A        string

	client.Connection
}

// New todo
func New(name string, debug bool, endpoint string, sslHost string, sslSkipVerify bool, verbose bool) (*Client, error) {
	if debug {
		gesclient.Debug()
	}
	settingsBuilder := client.CreateConnectionSettings()

	var uri *url.URL
	var err error
	if !strings.Contains(endpoint, "://") {
		gossipSeeds := strings.Split(endpoint, ",")
		endpoints := make([]*net.TCPAddr, len(gossipSeeds))
		for i, gossipSeed := range gossipSeeds {
			endpoints[i], err = net.ResolveTCPAddr("tcp", gossipSeed)
			if err != nil {
				log.Fatalf("Error resolving: %v", gossipSeed)
			}
		}
		settingsBuilder.SetGossipSeedEndPoints(endpoints)
	} else {
		uri, err = url.Parse(endpoint)
		if err != nil {
			log.Fatalf("Error parsing address: %v", err)
		}

		if uri.User != nil {
			username := uri.User.Username()
			password, _ := uri.User.Password()
			settingsBuilder.SetDefaultUserCredentials(client.NewUserCredentials(username, password))
		}
	}

	if sslHost != "" {
		settingsBuilder.UseSslConnection(sslHost, !sslSkipVerify)
	}

	if verbose {
		settingsBuilder.EnableVerboseLogging()
	}

	cli, err := gesclient.Create(settingsBuilder.Build(), uri, name)
	if err != nil {
		return nil, err
	}

	if err := cli.ConnectAsync().Wait(); err != nil {
		return nil, err
	}

	return &Client{
		EsClient: cli,
	}, nil
}

// Connected todo
func (s *Client) Connected() client.EventHandlers {
	return s.EsClient.Connected()
}

// Disconnected todo
func (s *Client) Disconnected() client.EventHandlers {
	return s.EsClient.Disconnected()
}

// Reconnecting todo
func (s *Client) Reconnecting() client.EventHandlers {
	return s.EsClient.Reconnecting()
}

// Closed todo
func (s *Client) Closed() client.EventHandlers {
	return s.EsClient.Closed()
}

// ErrorOccurred todo
func (s *Client) ErrorOccurred() client.EventHandlers {
	return s.EsClient.ErrorOccurred()
}

// AuthenticationFailed todo
func (s *Client) AuthenticationFailed() client.EventHandlers {
	return s.EsClient.AuthenticationFailed()
}

// SubscribeToStreamAsync todo
func (s *Client) SubscribeToStreamAsync(
	stream string,
	resolveLinkTos bool,
	eventAppeared client.EventAppearedHandler,
	subscriptionDropped client.SubscriptionDroppedHandler,
	userCredentials *client.UserCredentials,
) (*tasks.Task, error) {
	return s.EsClient.SubscribeToStreamAsync(stream, resolveLinkTos, eventAppeared, subscriptionDropped, userCredentials)
}

// ConnectToPersistentSubscriptionAsync todo
func (s *Client) ConnectToPersistentSubscriptionAsync(
	stream string,
	groupName string,
	eventAppeared client.PersistentEventAppearedHandler,
	subscriptionDropped client.PersistentSubscriptionDroppedHandler,
	userCredentials *client.UserCredentials,
	bufferSize int,
	autoAck bool,
) (*tasks.Task, error) {
	return s.EsClient.ConnectToPersistentSubscriptionAsync(stream, groupName, eventAppeared, subscriptionDropped, userCredentials, bufferSize, autoAck)
}

// AppendToStreamAsync todo
func (s *Client) AppendToStreamAsync(
	stream string,
	expectedVersion int,
	events []*client.EventData,
	userCredentials *client.UserCredentials,
) (*tasks.Task, error) {
	return s.EsClient.AppendToStreamAsync(stream, expectedVersion, events, userCredentials)
}
