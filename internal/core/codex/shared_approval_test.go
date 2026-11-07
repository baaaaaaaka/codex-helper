package codex

import (
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type replySink chan []byte

func (r replySink) Write(b []byte) (int, error) { r <- append([]byte(nil), b...); return len(b), nil }
func (r replySink) Close() error                { return nil }

func TestSharedApprovalTransfersWhenTUICloses(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "answered-by-tui"}[resolved], func(t *testing.T) {
			var attached atomic.Bool
			attached.Store(true)
			ignored := make(chan struct{}, 1)
			replies := make(replySink, 2)
			s := &Server{in: replies, cfg: Config{Log: io.Discard, IgnoreServerRequest: func(json.RawMessage) bool {
				if attached.Load() {
					ignored <- struct{}{}
					return true
				}
				return false
			}}}
			s.handleLine([]byte(`{"id":19,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread"}}`))
			select {
			case <-ignored:
			case <-time.After(time.Second):
				t.Fatal("approval not delegated")
			}
			if resolved {
				s.handleLine([]byte(`{"method":"serverRequest/resolved","params":{"threadId":"thread","requestId":19}}`))
			}
			attached.Store(false)
			s.AnswerPendingRequests("another-thread")
			s.AnswerPendingRequests("thread")
			s.AnswerPendingRequests("thread")
			s.wg.Wait()
			want := 1
			if resolved {
				want = 0
			}
			if len(replies) != want {
				t.Fatalf("replies=%d, want %d", len(replies), want)
			}
			if want == 1 {
				var reply wireMsg
				if err := json.Unmarshal(<-replies, &reply); err != nil || string(reply.ID) != "19" || reply.Error != nil {
					t.Fatalf("reply=%+v %v", reply, err)
				}
			}
			s.mu.Lock()
			retained := len(s.serverRequests)
			s.mu.Unlock()
			if retained != 0 {
				t.Fatalf("retained %d resolved requests", retained)
			}
		})
	}
}
