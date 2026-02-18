package distributor_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

const (
	peer1ID distributor.NodeID = 1
	peer2ID distributor.NodeID = 2
)

// Sends a message from p1 to p2.
func TestTransportSendSingle(t *testing.T) {
	const NumPeer uint = 2

	sendSingle := func(t *testing.T,
		p1, p2 distributor.Transport) {
		t.Helper()

		if err := p1.Connect(peer2ID); err != nil {
			t.Fatal(err)
		}

		p1.Send(peer2ID, distributor.NewSimpleMsg(p1.GetAddress(), "hi from peer1"))

		select {
		case msg := <-p2.Deliver():
			t.Log(msg)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message delivery")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport(fmt.Sprint(peer1ID), NumPeer)
		p2 := distributor.NewInmemTransport(fmt.Sprint(peer2ID), NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendSingle(t, p1, p2)
	})
	t.Run("tcp", func(t *testing.T) {
		addrs := distributor.AddrRegistory{
			peer1ID: ":8080",
			peer2ID: ":8081",
		}
		p1 := distributor.NewTcpTransport(addrs[peer1ID], NumPeer, addrs)
		p2 := distributor.NewTcpTransport(addrs[peer2ID], NumPeer, addrs)
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

		if err := p1.Connect(peer2ID); err != nil {
			t.Fatal(err)
		}

		go func() {
			for msg := range p2.Deliver() {
				t.Log(msg)
				msgs = append(msgs, msg)
			}
		}()

		sendMsgs := []distributor.Message{
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
			distributor.NewSimpleMsg(p1.GetAddress(), "hi"),
		}

		for _, sendMsg := range sendMsgs {
			p1.Send(peer2ID, sendMsg)
		}

		time.Sleep(time.Second)
		if !reflect.DeepEqual(sendMsgs, msgs) {
			t.Error("msgs list not equal")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport(fmt.Sprint(peer1ID), NumPeer)
		p2 := distributor.NewInmemTransport(fmt.Sprint(peer2ID), NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		sendThree(t, p1, p2)
	})

	t.Run("tcp", func(t *testing.T) {
		addrs := distributor.AddrRegistory{
			peer1ID: ":8080",
			peer2ID: ":8081",
		}
		p1 := distributor.NewTcpTransport(addrs[peer1ID], NumPeer, addrs)
		p2 := distributor.NewTcpTransport(addrs[peer2ID], NumPeer, addrs)
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

		if err := p1.Connect(peer2ID); err != nil {
			t.Fatal(err)
		}

		p1.Broadcast(distributor.NewSimpleMsg(p1.GetAddress(), "broadcast value"))

		// p1DeliveredMsg := <-p1.Deliver()
		// fmt.Println(p1DeliveredMsg)

		select {
		case msg := <-p2.Deliver():
			t.Log(msg)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message delivery")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport(fmt.Sprint(peer1ID), NumPeer)
		p2 := distributor.NewInmemTransport(fmt.Sprint(peer2ID), NumPeer)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		broadcastSingle(t, p1, p2)
	})
	t.Run("tcp", func(t *testing.T) {
		addrs := distributor.AddrRegistory{
			peer1ID: ":8080",
			peer2ID: ":8081",
		}
		p1 := distributor.NewTcpTransport(addrs[peer1ID], NumPeer, addrs)
		p2 := distributor.NewTcpTransport(addrs[peer2ID], NumPeer, addrs)
		t.Cleanup(func() {
			p1.Close()
			p2.Close()
		})
		broadcastSingle(t, p1, p2)
	})
}
