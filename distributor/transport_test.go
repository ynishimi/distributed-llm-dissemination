package distributor_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ynishimi/distributed-llm-dissemination/distributor"
	"github.com/ynishimi/distributed-llm-dissemination/distributor/testutil"
)

// Sends a message from p1 to p2.
func TestInmemoryTransportSendSingle(t *testing.T) {
	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1")
		p2 := distributor.NewInmemTransport("peer2")
		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}

		p1.Send(p2.GetAddress(), testutil.NewTestMsg(p1.GetAddress(), "hi from peer1"))

		select {
		case msg := <-p2.Deliver():
			fmt.Print(msg)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message delivery")
		}
	})
}

// Sends three messages from p1 to p2.
func TestInmemoryTransportSendThree(t *testing.T) {
	sendThree := func(t *testing.T, p1Send func(string, distributor.Message) error, p1Addr string, p2Deliver <-chan distributor.Message) {
		t.Helper()
		msgs := make([]distributor.Message, 0)

		go func() {
			for {
				msg := <-p2Deliver
				fmt.Println(msg)
				msgs = append(msgs, msg)
			}
		}()

		sendMsgs := []distributor.Message{
			testutil.NewTestMsg(p1Addr, "hi"),
			testutil.NewTestMsg(p1Addr, "hi"),
			testutil.NewTestMsg(p1Addr, "hi"),
		}

		for _, sendMsg := range sendMsgs {
			p1Send("peer2", sendMsg)
		}

		time.Sleep(time.Second)
		if !reflect.DeepEqual(sendMsgs, msgs) {
			t.Error("msgs list not equal")
		}
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1")
		p2 := distributor.NewInmemTransport("peer2")
		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}
		sendThree(t, p1.Send, p1.GetAddress(), p2.Deliver())
	})
}

func TestInmemoryTransportBroadcastSingle(t *testing.T) {
	broadcastSingle := func(t *testing.T, broadcast func(distributor.Message) error, addr string, p1Deliver, p2Deliver <-chan distributor.Message) {
		t.Helper()
		broadcast(testutil.NewTestMsg(addr, "broadcast value"))

		p1DeliveredMsg := <-p1Deliver
		fmt.Println(p1DeliveredMsg)
		p2DeliveredMsg := <-p2Deliver
		fmt.Println(p2DeliveredMsg)
	}

	t.Run("inmem", func(t *testing.T) {
		p1 := distributor.NewInmemTransport("peer1")
		p2 := distributor.NewInmemTransport("peer2")
		if err := p1.Connect(p2.GetAddress()); err != nil {
			t.Fatal(err)
		}
		broadcastSingle(t, p1.Broadcast, p1.GetAddress(), p1.Deliver(), p2.Deliver())
	})
}
