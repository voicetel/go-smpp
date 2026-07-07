package smpp

import (
	"testing"
	"time"

	"github.com/voicetel/go-smpp/smpp/pdu"
	"github.com/voicetel/go-smpp/smpp/pdu/pdufield"
	"github.com/voicetel/go-smpp/smpp/pdu/pdutext"
	"github.com/voicetel/go-smpp/smpp/smpptest"
)

// Close must ALWAYS terminate the bind loop and close the Status
// channel, even when the server has PDUs in flight at close time.
//
// Regression: Close() closes c.stop first, which kills the
// transceiver's dispatcher — the steady reader of the unbuffered
// inbox. The bind loop's inbox sends were unguarded, so an in-flight
// deliver_sm (or the unbind_resp itself, outside Close's one-PDU 1s
// window) blocked the bind goroutine forever: close(c.Status) never
// ran and the client leaked as a zombie session. Caught by
// smppGateway's TestCheckAndUpdateConnectionState_ClosesUnderlyingClient
// timing out at 20s under whole-repo load.
//
// The server side floods deliver_sm while the client closes, making
// the in-flight-PDU window easy to hit across 25 iterations.
func TestClose_WithInflightPDUs_ClosesStatusChannel(t *testing.T) {
	for i := 0; i < 25; i++ {
		flood := make(chan struct{})
		srv := smpptest.NewUnstartedServer()
		srv.Handler = func(cli smpptest.Conn, p pdu.Body) {
			switch p.Header().ID {
			case pdu.SubmitSMID:
				smpptest.EchoHandler(cli, p)
			case pdu.UnbindID:
				// Reply like a real SMSC, then keep flooding a few
				// more deliver_sm so a PDU is in flight while the
				// client tears down.
				_ = cli.Write(pdu.NewUnbindRespSeq(p.Header().Seq))
			default:
				smpptest.EchoHandler(cli, p)
			}
		}
		srv.Start()

		tx := &Transceiver{
			Addr:    srv.Addr(),
			User:    smpptest.DefaultUser,
			Passwd:  smpptest.DefaultPasswd,
			Handler: func(p pdu.Body) {}, // discard MOs
		}
		status := tx.Bind()
		select {
		case st := <-status:
			if st.Status() != Connected {
				t.Fatalf("iter %d: bind failed: %v", i, st.Error())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: bind never completed", i)
		}

		// Flood deliver_sm at the client from a server goroutine so
		// PDUs are on the wire when Close runs.
		go func() {
			defer close(flood)
			for j := 0; j < 50; j++ {
				p := pdu.NewDeliverSM()
				f := p.Fields()
				_ = f.Set(pdufield.SourceAddr, "12125550142")
				_ = f.Set(pdufield.DestinationAddr, "15551234567")
				_ = f.Set(pdufield.ShortMessage, pdutext.Raw([]byte("inflight")))
				srv.BroadcastMessage(p)
			}
		}()

		_ = tx.Close()

		// The Status channel MUST close; a zombie bind loop keeps it
		// open forever.
		deadline := time.After(5 * time.Second)
		closed := false
		for !closed {
			select {
			case _, ok := <-status:
				if !ok {
					closed = true
				}
			case <-deadline:
				t.Fatalf("iter %d: Status channel never closed — zombie bind loop", i)
			}
		}
		<-flood
		srv.Close()
	}
}
