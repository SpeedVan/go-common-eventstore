package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/SpeedVan/go-common-eventstore/client/eventstore"
	"github.com/jdextraze/go-gesclient/client"
)

func main() {
	c, err := eventstore.New("node_1", false, "tcp://admin:changeit@10.10.139.35:1113", "", false, false)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}

	c.Connected().Add(func(evt client.Event) error { log.Printf("Connected: %+v", evt); return nil })
	c.Disconnected().Add(func(evt client.Event) error { log.Printf("Disconnected: %+v", evt); return nil })
	c.Reconnecting().Add(func(evt client.Event) error { log.Printf("Reconnecting: %+v", evt); return nil })
	c.Closed().Add(func(evt client.Event) error { log.Fatalf("Connection closed: %+v", evt); return nil })
	c.ErrorOccurred().Add(func(evt client.Event) error { log.Printf("Error: %+v", evt); return nil })
	c.AuthenticationFailed().Add(func(evt client.Event) error { log.Printf("Auth failed: %+v", evt); return nil })

	// if err := c.ConnectAsync().Wait(); err != nil {
	// 	log.Fatalf("Error connecting: %v", err)
	// }
	task, err := c.ConnectToPersistentSubscriptionAsync("Default", "script_1", eventAppeared, subscriptionDropped, nil, 10, true)
	// task, err := c.SubscribeToStreamAsync("Default", true, eventAppeared, subscriptionDropped, nil)
	if err != nil {
		log.Printf("Error occured while subscribing to stream: %v", err)
	} else if err := task.Error(); err != nil {
		log.Printf("Error occured while waiting for result of subscribing to stream: %v", err)
	} else {
		sub := task.Result().(client.PersistentSubscription)
		log.Printf("SubscribeToStream result: %+v", sub)

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		<-ch

		sub.Stop()
	}

	c.Close()
	time.Sleep(10 * time.Millisecond)
}

func eventAppeared(_ client.PersistentSubscription, e *client.ResolvedEvent) error {
	log.Printf("event appeared: %s", string(e.Event().Data()))
	return nil
}

func subscriptionDropped(_ client.PersistentSubscription, r client.SubscriptionDropReason, err error) error {
	log.Printf("subscription dropped: %s, %v", r, err)
	return nil
}
