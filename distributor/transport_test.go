package distributor_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

// Sends a message from p1 to p2.
func TestTransportSendSingle(t *testing.T) {
	const NumPeer uint = 2

	sendSingle := func(t *testing.T,
		p1, p2 distributor.Transport) {
		t.Helper()

		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}

		p1.Send(p2.GetAddress(), distributor.NewSimpleMsg(p1.GetAddress(), "hi from peer1"))

		select {
		case msg := <-p2.Deliver():
			fmt.Print(msg)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message delivery")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1", NumPeer)
		p2 := distributor.NewInmemTransport("peer2", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendSingle(t, p1, p2)
	})
	t.Run("tcp", func(t *testing.T) {
		p1 := distributor.NewTcpTransport(":8080", NumPeer)
		p2 := distributor.NewTcpTransport(":8081", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendSingle(t, p1, p2)
	})
}

// Sends three messages from p1 to p2.
func TestInmemoryTransportSendThree(t *testing.T) {
	const NumPeer = 2

	sendThree := func(t *testing.T, p1, p2 distributor.Transport) {
		t.Helper()

		msgs := make([]distributor.Message, 0)

		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}

		go func() {
			for msg := range p2.Deliver() {
				fmt.Println(msg)
				msgs = append(msgs, msg)
			}
		}()

		sendMsgs := []distributor.Message{
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
		}

		for _, sendMsg := range sendMsgs {
			p1.Send(p2.GetAddress(), sendMsg)
		}

		time.Sleep(time.Second)
		if !reflect.DeepEqual(sendMsgs, msgs) {
			t.Error("msgs list not equal")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1", NumPeer)
		p2 := distributor.NewInmemTransport("peer2", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendThree(t, p1, p2)
	})

	t.Run("tcp", func(t *testing.T) {
		p1 := distributor.NewTcpTransport(":8080", NumPeer)
		p2 := distributor.NewTcpTransport(":8081", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendThree(t, p1, p2)
	})
}

func TestInmemoryTransportBroadcastSingle(t *testing.T) {
	const NumPeer = 2
	broadcastSingle := func(t *testing.T, p1, p2 distributor.Transport) {
		t.Helper()

		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}

		p1.Broadcast(distributor.NewSimpleMsg(p1.GetAddress(), "broadcast value"))

		// p1DeliveredMsg := <-p1.Deliver()
		// fmt.Println(p1DeliveredMsg)
		p2DeliveredMsg := <-p2.Deliver()
		fmt.Println(p2DeliveredMsg)
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1", NumPeer)
		p2 := distributor.NewInmemTransport("peer2", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		broadcastSingle(t, p1, p2)
	})
	t.Run("tcp", func(t *testing.T) {
		p1 := distributor.NewTcpTransport(":8080", NumPeer)
		p2 := distributor.NewTcpTransport(":8081", NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		broadcastSingle(t, p1, p2)
	})
}
