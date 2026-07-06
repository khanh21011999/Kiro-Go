package proxy

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"kiro-go/config"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

func resetEndpointQuotaCooldownsForTest() {
	endpointQuotaCooldowns = sync.Map{}
	endpointLastSuccesses = sync.Map{}
}

func TestCallKiroAPIRetriesNextEndpointWhenStreamFailsBeforeOutput(t *testing.T) {
	resetEndpointQuotaCooldownsForTest()
	cfgFile := t.TempDir() + "/config.json"
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdatePreferredEndpoint("kiro"); err != nil {
		t.Fatalf("set preferred endpoint: %v", err)
	}
	if err := config.UpdateEndpointFallback(true); err != nil {
		t.Fatalf("enable endpoint fallback: %v", err)
	}

	var hits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		switch r.URL.Path {
		case "/broken-stream":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte{0, 0, 0, 20})
		case "/ok":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "fallback worked"}))
		default:
			http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	oldEndpoints := kiroEndpoints
	kiroEndpoints = []kiroEndpoint{
		{URL: server.URL + "/broken-stream", Origin: "AI_EDITOR", Name: "broken"},
		{URL: server.URL + "/ok", Origin: "AI_EDITOR", Name: "ok"},
		{URL: server.URL + "/unused", Origin: "AI_EDITOR", Name: "unused"},
	}
	defer func() { kiroEndpoints = oldEndpoints }()

	payload := &KiroPayload{}
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: "hello",
		Origin:  "AI_EDITOR",
	}

	var got string
	err := CallKiroAPI(&config.Account{
		AccessToken: "token",
		ProfileArn:  "arn:aws:codewhisperer:us-east-1:123456789012:profile/test",
	}, payload, &KiroStreamCallback{
		OnText: func(text string, _ bool) { got += text },
	})
	if err != nil {
		t.Fatalf("expected fallback endpoint to succeed, got error: %v", err)
	}
	if got != "fallback worked" {
		t.Fatalf("expected fallback text, got %q", got)
	}
	if len(hits) != 2 || hits[0] != "/broken-stream" || hits[1] != "/ok" {
		t.Fatalf("expected broken endpoint then fallback endpoint, got %#v", hits)
	}
}

func TestCallKiroAPIDoesNotRetryStreamFailureAfterOutput(t *testing.T) {
	resetEndpointQuotaCooldownsForTest()
	cfgFile := t.TempDir() + "/config.json"
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdatePreferredEndpoint("kiro"); err != nil {
		t.Fatalf("set preferred endpoint: %v", err)
	}
	if err := config.UpdateEndpointFallback(true); err != nil {
		t.Fatalf("enable endpoint fallback: %v", err)
	}

	var hits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		switch r.URL.Path {
		case "/partial-stream":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "partial"}))
			_, _ = w.Write([]byte{0, 0, 0, 20})
		case "/ok":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "fallback should not run"}))
		default:
			http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	oldEndpoints := kiroEndpoints
	kiroEndpoints = []kiroEndpoint{
		{URL: server.URL + "/partial-stream", Origin: "AI_EDITOR", Name: "partial"},
		{URL: server.URL + "/ok", Origin: "AI_EDITOR", Name: "ok"},
		{URL: server.URL + "/unused", Origin: "AI_EDITOR", Name: "unused"},
	}
	defer func() { kiroEndpoints = oldEndpoints }()

	payload := &KiroPayload{}
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: "hello",
		Origin:  "AI_EDITOR",
	}

	var got string
	err := CallKiroAPI(&config.Account{
		AccessToken: "token",
		ProfileArn:  "arn:aws:codewhisperer:us-east-1:123456789012:profile/test",
	}, payload, &KiroStreamCallback{
		OnText: func(text string, _ bool) { got += text },
	})
	if err == nil {
		t.Fatalf("expected stream error after partial output")
	}
	if got != "partial" {
		t.Fatalf("expected partial output to be preserved, got %q", got)
	}
	if len(hits) != 1 || hits[0] != "/partial-stream" {
		t.Fatalf("expected no retry after output, got hits %#v", hits)
	}
}

func TestCallKiroAPISkipsEndpointAfterQuotaCooldown(t *testing.T) {
	resetEndpointQuotaCooldownsForTest()
	cfgFile := t.TempDir() + "/config.json"
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdatePreferredEndpoint("kiro"); err != nil {
		t.Fatalf("set preferred endpoint: %v", err)
	}
	if err := config.UpdateEndpointFallback(true); err != nil {
		t.Fatalf("enable endpoint fallback: %v", err)
	}

	var hits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		switch r.URL.Path {
		case "/quota":
			http.Error(w, "quota", http.StatusTooManyRequests)
		case "/ok":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "ok"}))
		default:
			http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	oldEndpoints := kiroEndpoints
	kiroEndpoints = []kiroEndpoint{
		{URL: server.URL + "/quota", Origin: "AI_EDITOR", Name: "quota"},
		{URL: server.URL + "/ok", Origin: "AI_EDITOR", Name: "ok"},
		{URL: server.URL + "/unused", Origin: "AI_EDITOR", Name: "unused"},
	}
	defer func() {
		kiroEndpoints = oldEndpoints
		resetEndpointQuotaCooldownsForTest()
	}()

	payload := &KiroPayload{}
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: "hello",
		Origin:  "AI_EDITOR",
	}
	account := &config.Account{
		ID:          "acct",
		AccessToken: "token",
		ProfileArn:  "arn:aws:codewhisperer:us-east-1:123456789012:profile/test",
	}

	err := CallKiroAPI(account, payload, &KiroStreamCallback{})
	if err != nil {
		t.Fatalf("first call should fall back after quota, got %v", err)
	}
	if len(hits) != 2 || hits[0] != "/quota" || hits[1] != "/ok" {
		t.Fatalf("expected first call to hit quota then ok, got %#v", hits)
	}

	hits = nil
	var skipped bool
	err = CallKiroAPI(account, payload, &KiroStreamCallback{
		OnEndpointAttempt: func(attempt KiroEndpointAttempt) {
			if attempt.Name == "quota" && attempt.Skipped {
				skipped = true
			}
		},
	})
	if err != nil {
		t.Fatalf("second call should skip quota and succeed, got %v", err)
	}
	if !skipped {
		t.Fatalf("expected quota endpoint to be reported as skipped")
	}
	if len(hits) != 1 || hits[0] != "/ok" {
		t.Fatalf("expected second call to skip quota endpoint and hit ok only, got %#v", hits)
	}
}

func TestCallKiroAPIPrefersLastSuccessfulEndpointInAutoMode(t *testing.T) {
	resetEndpointQuotaCooldownsForTest()
	cfgFile := t.TempDir() + "/config.json"
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdatePreferredEndpoint("auto"); err != nil {
		t.Fatalf("set preferred endpoint: %v", err)
	}
	if err := config.UpdateEndpointFallback(true); err != nil {
		t.Fatalf("enable endpoint fallback: %v", err)
	}

	var hits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		switch r.URL.Path {
		case "/slow-failure":
			http.Error(w, "try another", http.StatusInternalServerError)
		case "/ok":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "ok"}))
		default:
			http.Error(w, "unexpected endpoint", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	oldEndpoints := kiroEndpoints
	kiroEndpoints = []kiroEndpoint{
		{URL: server.URL + "/slow-failure", Origin: "AI_EDITOR", Name: "slow"},
		{URL: server.URL + "/ok", Origin: "AI_EDITOR", Name: "ok"},
	}
	defer func() {
		kiroEndpoints = oldEndpoints
		resetEndpointQuotaCooldownsForTest()
	}()

	payload := &KiroPayload{}
	payload.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: "hello",
		Origin:  "AI_EDITOR",
	}
	account := &config.Account{
		ID:          "acct",
		AccessToken: "token",
		ProfileArn:  "arn:aws:codewhisperer:us-east-1:123456789012:profile/test",
	}

	if err := CallKiroAPI(account, payload, &KiroStreamCallback{}); err != nil {
		t.Fatalf("first call should fall back to ok endpoint, got %v", err)
	}
	if len(hits) != 2 || hits[0] != "/slow-failure" || hits[1] != "/ok" {
		t.Fatalf("expected first call to learn ok endpoint after fallback, got %#v", hits)
	}

	hits = nil
	if err := CallKiroAPI(account, payload, &KiroStreamCallback{}); err != nil {
		t.Fatalf("second call should use remembered ok endpoint, got %v", err)
	}
	if len(hits) != 1 || hits[0] != "/ok" {
		t.Fatalf("expected second call to prefer last successful endpoint, got %#v", hits)
	}
}

func TestNormalizeChunkBasicProgression(t *testing.T) {
	prev := ""

	if got := normalizeChunk("abc", &prev); got != "abc" {
		t.Fatalf("expected first chunk to pass through, got %q", got)
	}
	if got := normalizeChunk("abcde", &prev); got != "de" {
		t.Fatalf("expected appended delta, got %q", got)
	}
}

func TestNormalizeChunkPrefixRewindDoesNotReplay(t *testing.T) {
	prev := ""

	_ = normalizeChunk("abcde", &prev)
	if got := normalizeChunk("abc", &prev); got != "" {
		t.Fatalf("expected rewind chunk to be ignored, got %q", got)
	}
	if prev != "abcde" {
		t.Fatalf("expected previous snapshot to remain longest version, got %q", prev)
	}
	if got := normalizeChunk("abcdef", &prev); got != "f" {
		t.Fatalf("expected only unseen suffix after rewind, got %q", got)
	}
}

func TestNormalizeChunkOverlapDelta(t *testing.T) {
	prev := "hello world"

	if got := normalizeChunk("world!!!", &prev); got != "!!!" {
		t.Fatalf("expected overlap suffix delta, got %q", got)
	}
}

func TestParseEventStreamFinishesPendingToolUseOnEOF(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
		"toolUseId": "toolu_1",
		"name":      "mcpIdaProMcpStatus",
		"input":     `{"server":"ida-pro-mcp"}`,
	}))

	var toolUses []KiroToolUse
	var completed bool
	err := parseEventStream(stream, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
		OnComplete: func(_, _ int) {
			completed = true
		},
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !completed {
		t.Fatalf("expected stream completion callback")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected pending tool use to be emitted on EOF, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_1" || toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool use: %#v", toolUses[0])
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected parsed tool input, got %#v", toolUses[0].Input)
	}
}

func TestParseEventStreamNilCallbackIsNoOp(t *testing.T) {
	stream := bytes.NewReader(bytes.Join([][]byte{
		awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "hello"}),
		awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{"text": "thinking"}),
		awsEventStreamFrame(t, "contextUsageEvent", map[string]interface{}{"contextUsagePercentage": 12.5}),
		awsEventStreamFrame(t, "meteringEvent", map[string]interface{}{"usage": 1.25}),
		awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
			"name":  "mcpIdaProMcpStatus",
			"input": `{"server":"ida-pro-mcp"}`,
			"stop":  true,
		}),
	}, nil))

	if err := parseEventStream(stream, nil); err != nil {
		t.Fatalf("expected nil callback to be a no-op, got %v", err)
	}
}

func TestParseEventStreamNilCallbackFieldsAreNoOp(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
		"content": "hello",
	}))

	if err := parseEventStream(stream, &KiroStreamCallback{}); err != nil {
		t.Fatalf("expected empty callback to be a no-op, got %v", err)
	}
}

func TestHandleToolUseEventGeneratesMissingToolUseID(t *testing.T) {
	var toolUses []KiroToolUse
	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":"ida-pro-mcp"}`,
		"stop":  true,
	}, nil, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	})

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID == "" {
		t.Fatalf("expected generated tool use id")
	}
	if toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool name: %q", toolUses[0].Name)
	}
}

func TestHandleToolUseEventReplacesGeneratedIDWhenRealIDArrives(t *testing.T) {
	var toolUses []KiroToolUse
	callback := &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	}

	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":`,
	}, nil, callback)
	current = handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_real",
		"name":      "mcpIdaProMcpStatus",
		"input":     `"ida-pro-mcp"}`,
		"stop":      true,
	}, current, callback)

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one completed tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_real" {
		t.Fatalf("expected real tool id to replace generated id, got %q", toolUses[0].ToolUseID)
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected joined tool input, got %#v", toolUses[0].Input)
	}
}

func TestBuildKiroTransportUsesExplicitProxyURL(t *testing.T) {
	transport := buildKiroTransport("http://proxy.local:8080")
	req := &http.Request{URL: mustParseURL(t, "https://q.us-east-1.amazonaws.com")}

	got, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("unexpected proxy error: %v", err)
	}
	assertProxyURL(t, got, "http://proxy.local:8080")
}

func TestBuildKiroTransportFallsBackToEnvironmentProxy(t *testing.T) {
	transport := buildKiroTransport("")
	if transport.Proxy == nil {
		t.Fatalf("expected empty proxy config to install environment proxy support")
	}
}

func TestInitKiroHttpClientKeepsShortRestTimeout(t *testing.T) {
	InitKiroHttpClient("")
	t.Cleanup(func() { InitKiroHttpClient("") })

	streamClient := kiroHttpStore.Load()
	restClient := kiroRestHttpStore.Load()

	if streamClient.Timeout != 5*time.Minute {
		t.Fatalf("expected streaming timeout to be 5m, got %s", streamClient.Timeout)
	}
	if restClient.Timeout != 30*time.Second {
		t.Fatalf("expected REST timeout to stay 30s, got %s", restClient.Timeout)
	}
}

func TestSetPayloadProfileArnForAccountUsesAccountArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: "arn:aws:codewhisperer:profile/stale"}

	setPayloadProfileArnForAccount(payload, &config.Account{ProfileArn: " arn:aws:codewhisperer:profile/current "})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/current" {
		t.Fatalf("expected current account profile ARN, got %q", payload.ProfileArn)
	}
}

func TestSetPayloadProfileArnForAccountPreservesExplicitPayloadArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: " arn:aws:codewhisperer:profile/explicit "}

	setPayloadProfileArnForAccount(payload, &config.Account{})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/explicit" {
		t.Fatalf("expected explicit payload profile ARN to be preserved, got %q", payload.ProfileArn)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("invalid test URL: %v", err)
	}
	return parsed
}

func assertProxyURL(t *testing.T, got *url.URL, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected proxy URL %q, got nil", want)
	}
	if got.String() != want {
		t.Fatalf("expected proxy URL %q, got %q", want, got.String())
	}
}

func awsEventStreamFrame(t *testing.T, eventType string, payload map[string]interface{}) []byte {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	headerValue := []byte(eventType)
	headers := make([]byte, 0, 1+len(":event-type")+1+2+len(headerValue))
	headers = append(headers, byte(len(":event-type")))
	headers = append(headers, []byte(":event-type")...)
	headers = append(headers, byte(7))
	headers = append(headers, byte(len(headerValue)>>8), byte(len(headerValue)))
	headers = append(headers, headerValue...)

	totalLength := 12 + len(headers) + len(payloadBytes) + 4
	frame := make([]byte, 12, totalLength)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(headers)))
	frame = append(frame, headers...)
	frame = append(frame, payloadBytes...)
	frame = append(frame, 0, 0, 0, 0)
	return frame
}
