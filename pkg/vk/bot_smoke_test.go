package vk

// Smoke test: BotClient against a stub VK API server. Verifies the request
// plumbing after the port (GET/POST form encoding, long-poll URL, update
// parsing, message send + chunking, event answers).

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type sentMessage struct {
	peerID   int64
	message  string
	keyboard string
}

type stubVK struct {
	mu        sync.Mutex
	sent      []sentMessage
	eventAnsw []string
	lpCalls   int
}

func (s *stubVK) recordSent(peerID int64, message, keyboard string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, sentMessage{peerID: peerID, message: message, keyboard: keyboard})
}

func (s *stubVK) recordEventAnswer(eventID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventAnsw = append(s.eventAnsw, eventID)
}

func (s *stubVK) countSent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func newStubVK(t *testing.T) (*httptest.Server, *stubVK, string) {
	t.Helper()
	s := &stubVK{}
	lpPath := "/lp"
	var lpURL string // resolved after the test server starts

	mux := http.NewServeMux()
	mux.HandleFunc("/method/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// NOTE: sendMessageEventAnswer must be matched before messages.send
		// (substring).
		switch {
		case strings.Contains(p, "sendMessageEventAnswer"):
			r.ParseForm()
			s.recordEventAnswer(r.Form.Get("event_id"))
			fmt.Fprint(w, `{"response":1}`)
		case strings.Contains(p, "groups.getById"):
			fmt.Fprint(w, `{"response":{"groups":[{"id":222}]}}`)
		case strings.Contains(p, "getLongPollServer"):
			fmt.Fprintf(w, `{"response":{"server":"%s","key":"key1","ts":"7"}}`, lpURL)
		case strings.Contains(p, "messages.send"):
			r.ParseForm()
			s.recordSent(parseID(r.Form.Get("peer_id")), r.Form.Get("message"), r.Form.Get("keyboard"))
			fmt.Fprint(w, `{"response":999}`)
		case strings.Contains(p, "messages.getById"):
			fmt.Fprint(w, `{"response":{"items":[{"id":501,"peer_id":1001,"text":"full text"}]}}`)
		default:
			fmt.Fprint(w, `{"response":1}`)
		}
	})

	mux.HandleFunc(lpPath, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") == "bad" {
			fmt.Fprint(w, `{"failed":1,"ts":7,"updates":[]}`)
			return
		}
		s.mu.Lock()
		s.lpCalls++
		first := s.lpCalls == 1
		s.mu.Unlock()
		if first {
			fmt.Fprint(w, `{"ts":"8","failed":0,"updates":[`+
				`{"type":"message_new","group_id":222,"object":{"client_id":1,"message":{"id":501,"peer_id":1001,"from_id":1001,"date":1,"out":0,"text":"привет","attachments":[{"type":"photo","photo":{"id":9,"owner_id":3}}]}}},`+
				`{"type":"message_new","group_id":222,"object":{"client_id":2,"message":{"id":502,"peer_id":1001,"from_id":222,"date":2,"out":1,"text":"своё эхо"}}},`+
				`{"type":"message_event","group_id":222,"object":{"peer_id":1001,"user_id":1001,"event_id":"ev-1","payload":"{\"command\":\"model_switch\",\"ref\":\"ollama/llama\"}"}}`+
				`]}`)
			return
		}
		fmt.Fprint(w, `{"ts":"8","failed":0,"updates":[]}`)
	})

	srv := httptest.NewServer(mux)
	lpURL = srv.URL + lpPath
	t.Cleanup(srv.Close)
	return srv, s, srv.URL
}

func parseID(v string) int64 {
	var n int64
	for _, c := range v {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		}
	}
	return n
}

func clientFor(t *testing.T, srvURL string) *BotClient {
	t.Helper()
	c := NewBotClient("test-token")
	c.baseURL = srvURL + "/method/"
	c.httpClient.Timeout = 10 * time.Second
	return c
}

func TestVKClientFlow(t *testing.T) {
	_, stub, srvURL := newStubVK(t)
	c := clientFor(t, srvURL)

	// 1. groups.getById + groups.getLongPollServer
	server, key, ts, err := c.GetLongPollServer()
	if err != nil {
		t.Fatalf("GetLongPollServer: %v", err)
	}
	if key != "key1" || ts != 7 {
		t.Fatalf("unexpected long poll server: key=%q ts=%d", key, ts)
	}
	if !strings.Contains(server, "/lp") {
		t.Fatalf("unexpected server url: %q", server)
	}

	// 2. long poll: message_new (incoming, with attachment) + message_event
	msgs, newTs, err := c.CheckUpdates(context.Background(), server, key, ts)
	if err != nil {
		t.Fatalf("CheckUpdates: %v", err)
	}
	if newTs != 8 {
		t.Fatalf("ts not advanced: %d", newTs)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d: %+v", len(msgs), msgs)
	}
	m := msgs[0]
	if m.ID != 501 || m.PeerID != 1001 || m.FromID != 1001 || m.Text != "привет" {
		t.Fatalf("bad message_new parse: %+v", m)
	}
	if len(m.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(m.Attachments))
	}
	// Self-echo guard: a message_new with out=1 (a message the bot itself
	// sent) must never reach the handler.
	for _, m := range msgs {
		if m.ID == 502 || m.Text == "своё эхо" {
			t.Fatalf("own message (out=1) leaked into CheckUpdates: %+v", msgs)
		}
	}
	ev := msgs[1]
	if ev.EventID != "ev-1" || !strings.Contains(ev.Payload, "model_switch") {
		t.Fatalf("bad message_event parse: %+v", ev)
	}

	// 3. long poll failed (bad key)
	if _, _, err := c.CheckUpdates(context.Background(), server, "bad", ts); err == nil || !strings.Contains(err.Error(), "long poll failed") {
		t.Fatalf("expected 'long poll failed' error, got %v", err)
	}

	// 4. short message send
	if _, err := c.SendMessage(1001, "hello"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if n := stub.countSent(); n != 1 {
		t.Fatalf("expected 1 sent message, got %d", n)
	}

	// 5. long message -> chunked with [i/n] prefixes
	long := strings.Repeat("a", 5000)
	if _, err := c.SendMessage(1001, long); err != nil {
		t.Fatalf("SendMessage(long): %v", err)
	}
	if n := stub.countSent(); n != 4 { // 1 from step 4 + 3 chunks
		t.Fatalf("expected 4 total sent messages, got %d", n)
	}

	// 6. event answer
	if err := c.SendMessageEventAnswer("ev-1", 1001, 1001, ""); err != nil {
		t.Fatalf("SendMessageEventAnswer: %v", err)
	}
	stub.mu.Lock()
	gotAnswer := len(stub.eventAnsw) == 1 && stub.eventAnsw[0] == "ev-1"
	stub.mu.Unlock()
	if !gotAnswer {
		t.Fatalf("event answer not recorded: %+v", stub.eventAnsw)
	}

	// 7. GetMessagesByID
	items, err := c.GetMessagesByID([]int64{501})
	if err != nil {
		t.Fatalf("GetMessagesByID: %v", err)
	}
	if len(items) != 1 || items[0].Text != "full text" {
		t.Fatalf("bad GetMessagesByID parse: %+v", items)
	}
}
