package vk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"omp-vk-gateway/pkg/internalmsg"
	"omp-vk-gateway/pkg/logger"
)

type LongPollServerResponse struct {
	Server string `json:"server"`
	Key    string `json:"key"`
	Ts     string `json:"ts"`
}

type VKAttachment struct {
	Type string                 `json:"type"`
	Raw  map[string]interface{} `json:"-"`
}

func (a *VKAttachment) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal attachment: %w", err)
	}

	if typ, ok := raw["type"].(string); ok {
		a.Type = typ
	}

	a.Raw = make(map[string]interface{})
	for k, v := range raw {
		if k != "type" {
			a.Raw[k] = v
		}
	}

	return nil
}

func (a *VKAttachment) ToRaw() map[string]interface{} {
	result := make(map[string]interface{})
	result["type"] = a.Type
	for k, v := range a.Raw {
		result[k] = v
	}
	return result
}

type DownloadedAttachment struct {
	Type     string
	Path     string
	Filename string
}

type VKMessage struct {
	ID          int64          `json:"id"`
	PeerID      int64          `json:"peer_id"`
	FromID      int64          `json:"from_id"`
	Date        int64          `json:"date"`
	Text        string         `json:"text"`
	Payload     string         `json:"payload,omitempty"`
	EventID     string         `json:"event_id,omitempty"`
	Attachments []VKAttachment `json:"attachments,omitempty"`
}

type APIErrorResponse struct {
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_msg"`
}

type BotClient struct {
	token      string
	apiVersion string
	baseURL    string
	httpClient *http.Client
	groupID    int64
}

func NewBotClient(token string) *BotClient {
	c := &BotClient{
		token:      token,
		apiVersion: "5.200",
		baseURL:    "https://api.vk.com/method/",
	}
	c.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: c.dialViaDNSFallback,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
	return c
}

// dialViaDNSFallback connects to addr (host:port) resolving the host through
// the fallback DNS chain, so a broken systemd-resolved never blinds the
// gateway. Falls back to plain dialing for literal IPs.
func (c *BotClient) dialViaDNSFallback(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) == nil {
		ip, derr := c.resolveHost(host)
		if derr != nil {
			return nil, derr
		}
		addr = net.JoinHostPort(ip.String(), port)
	}
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// dnsFallbackChain tries the system resolver first, then public resolvers.
// systemd-resolved intermittently fails ("server misbehaving"), leaving the
// gateway blind until someone touches the network stack. Falling back keeps
// VK reachable regardless.
func (c *BotClient) resolveHost(host string) (net.IP, error) {
	resolvers := []struct {
		name string
		dns  *net.Resolver
	}{
		{"system", net.DefaultResolver},
		{"cloudflare", &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", "1.1.1.1:53")
		}}},
		{"google", &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", "8.8.8.8:53")
		}}},
	}
	var lastErr error
	for _, r := range resolvers {
		addrs, err := r.dns.LookupIPAddr(context.Background(), host)
		if err == nil && len(addrs) > 0 {
			ip := addrs[0].IP
			logger.DebugToFile("[dns] resolved %s -> %s via %s", host, ip.String(), r.name)
			return ip, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("all DNS resolvers failed for %s: %w", host, lastErr)
}

// retryTransient retries fn on transient network/DNS errors with exponential
// backoff. VK API rate-limit and auth errors are not retried.
func retryTransient(opName string, fn func() error) error {
	backoffs := []time.Duration{500 * time.Millisecond, 2 * time.Second, 5 * time.Second}
	var attemptErr error
	for i, b := range backoffs {
		attemptErr = fn()
		if attemptErr == nil {
			return nil
		}
		if !isTransientNetworkError(attemptErr) {
			break
		}
		logger.DebugToFile("[%s] transient error (attempt %d/%d): %v", opName, i+1, len(backoffs)+1, attemptErr)
		time.Sleep(b)
	}
	return attemptErr
}

func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"dial tcp:",
		"lookup ",
		"server misbehaving",
		"connection refused",
		"connection reset",
		"EOF",
		"context deadline exceeded",
		"TLS handshake timeout",
		"http2: timeout awaiting response headers",
		"i/o timeout",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func (c *BotClient) doRequestPOST(endpoint string, params map[string]interface{}) ([]byte, error) {
	endpointURL := fmt.Sprintf("%s%s", c.baseURL, endpoint)

	formBody := &bytes.Buffer{}
	for k, v := range params {
		if formBody.Len() > 0 {
			formBody.WriteString("&")
		}
		val := formatValue(v)
		formBody.WriteString(url.QueryEscape(k) + "=" + url.QueryEscape(val))
	}
	bodyBytes := formBody.Bytes()

	var responseBody []byte
	err := retryTransient("POST "+endpoint, func() error {
		req, e := http.NewRequest("POST", endpointURL, bytes.NewReader(bodyBytes))
		if e != nil {
			return fmt.Errorf("failed to create request: %w", e)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		r, e := c.httpClient.Do(req)
		if e != nil {
			return e
		}
		defer r.Body.Close()
		b, e := io.ReadAll(r.Body)
		if e != nil {
			return fmt.Errorf("failed to read response: %w", e)
		}
		if r.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: %d, body: %s", r.StatusCode, string(b[:min(500, len(b))]))
		}
		responseBody = b
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	var apiError struct {
		Error APIErrorResponse `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &apiError); err == nil && apiError.Error.ErrorCode > 0 {
		return nil, fmt.Errorf("VK API error %d: %s", apiError.Error.ErrorCode, apiError.Error.ErrorMessage)
	}

	return responseBody, nil
}

func (c *BotClient) doRequestGET(endpoint string, params map[string]interface{}) ([]byte, error) {
	reqURL := fmt.Sprintf("%s%s", c.baseURL, endpoint)

	query := "access_token=" + c.token + "&v=" + c.apiVersion

	for k, v := range params {
		if k == "access_token" || k == "v" {
			continue
		}
		val := formatValue(v)
		query += "&" + url.QueryEscape(k) + "=" + url.QueryEscape(val)
	}

	reqURL += "?" + query

	var responseBody []byte
	err := retryTransient("GET "+endpoint, func() error {
		r, e := c.httpClient.Get(reqURL)
		if e != nil {
			return e
		}
		defer r.Body.Close()
		b, e := io.ReadAll(r.Body)
		if e != nil {
			return fmt.Errorf("failed to read response: %w", e)
		}
		if r.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: %d", r.StatusCode)
		}
		responseBody = b
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	var apiError struct {
		Error APIErrorResponse `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &apiError); err == nil && apiError.Error.ErrorCode > 0 {
		return nil, fmt.Errorf("VK API error %d: %s", apiError.Error.ErrorCode, apiError.Error.ErrorMessage)
	}

	return responseBody, nil
}

func formatValue(v interface{}) string {
	if f, ok := v.(float64); ok && f == float64(int64(f)) {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%v", v)
}

func (c *BotClient) ensureGroupID() error {
	if c.groupID != 0 {
		return nil
	}

	responseBody, err := c.doRequestGET("groups.getById", map[string]interface{}{})
	if err != nil {
		return err
	}

	var response struct {
		Response struct {
			Groups []struct {
				ID int64 `json:"id"`
			} `json:"groups"`
		} `json:"response"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return fmt.Errorf("groups.getById: %w", err)
	}
	if len(response.Response.Groups) == 0 {
		return fmt.Errorf("groups.getById: no groups returned")
	}

	c.groupID = response.Response.Groups[0].ID
	return nil
}

func (c *BotClient) GetLongPollServer() (string, string, int64, error) {
	if err := c.ensureGroupID(); err != nil {
		return "", "", 0, err
	}

	params := map[string]interface{}{
		"group_id": c.groupID,
	}

	responseBody, err := c.doRequestGET("groups.getLongPollServer", params)
	if err != nil {
		return "", "", 0, err
	}

	var response struct {
		Response LongPollServerResponse `json:"response"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", "", 0, fmt.Errorf("failed to parse response: %w", err)
	}

	return response.Response.Server, response.Response.Key, toInt64(response.Response.Ts), nil
}

func (c *BotClient) CheckUpdates(ctx context.Context, server, key string, ts int64) ([]VKMessage, int64, error) {
	lpURL := fmt.Sprintf("%s?act=a_check&key=%s&ts=%d&wait=25", server, key, ts)

	req, err := http.NewRequestWithContext(ctx, "GET", lpURL, nil)
	if err != nil {
		return nil, ts, fmt.Errorf("create request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, ts, fmt.Errorf("long poll request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ts, fmt.Errorf("failed to read response: %w", err)
	}

	var result struct {
		Failed  int         `json:"failed"`
		Ts      interface{} `json:"ts"`
		Updates []lpUpdate  `json:"updates"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, ts, fmt.Errorf("failed to parse response: %w (body: %s)", err, string(responseBody[:min(200, len(responseBody))]))
	}

	newTs := toInt64(result.Ts)
	if result.Failed != 0 {
		// failed=2 means "your ts is behind, here is the current one"; the
		// caller can adopt newTs and keep going without a full reconnect.
		return nil, newTs, fmt.Errorf("long poll failed: code=%d", result.Failed)
	}

	var messages []VKMessage
	for _, update := range result.Updates {
		switch update.Type {
		case "message_new":
			msg := parseMessageNewUpdate(update.Object)
			if msg.FromID == 0 && msg.PeerID == 0 {
				continue
			}
			messages = append(messages, msg)

		case "message_event":
			messages = append(messages, parseMessageEventUpdate(update.Object))
		}
	}

	return messages, toInt64(result.Ts), nil
}

type lpUpdate struct {
	Type    string                 `json:"type"`
	Object  map[string]interface{} `json:"object"`
	GroupID int64                  `json:"group_id"`
}

func parseMessageNewUpdate(object map[string]interface{}) VKMessage {
	rawObjDump, _ := json.Marshal(object)
	logger.DebugToFile("[parseMessageNewUpdate] OBJECT DUMP: %s", string(rawObjDump))

	raw, ok := object["message"].(map[string]interface{})
	if !ok {
		return VKMessage{}
	}

	if out, ok := raw["out"].(float64); ok && int(out) == 1 {
		return VKMessage{}
	}

	id := toInt64(raw["id"])
	if id == 0 {
		id = toInt64(object["message_id"])
	}

	msg := VKMessage{
		ID:     id,
		PeerID: toInt64(raw["peer_id"]),
		FromID: toInt64(raw["from_id"]),
		Date:   toInt64(raw["date"]),
		Text:   toString(raw["text"]),
	}

	if atts, ok := raw["attachments"].([]interface{}); ok {
		for _, a := range atts {
			if amap, ok := a.(map[string]interface{}); ok {
				if b, err := json.Marshal(amap); err == nil {
					var att VKAttachment
					_ = json.Unmarshal(b, &att)
					msg.Attachments = append(msg.Attachments, att)
				}
			}
		}
	}

	return msg
}

func parseMessageEventUpdate(object map[string]interface{}) VKMessage {
	var payloadStr string
	switch p := object["payload"].(type) {
	case string:
		payloadStr = p
	case map[string]interface{}:
		if b, err := json.Marshal(p); err == nil {
			payloadStr = string(b)
		}
	}

	return VKMessage{
		ID:      int64(time.Now().UnixNano()),
		PeerID:  toInt64(object["peer_id"]),
		FromID:  toInt64(object["user_id"]),
		Date:    time.Now().Unix(),
		Payload: payloadStr,
		EventID: toString(object["event_id"]),
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func extractMessageID(response interface{}) (int64, error) {

	if msgID, ok := response.(float64); ok {
		return int64(msgID), nil
	}

	if arr, ok := response.([]interface{}); ok {
		if len(arr) > 0 {
			if msgMap, ok := arr[0].(map[string]interface{}); ok {
				if msgID, ok := msgMap["message_id"].(float64); ok {
					return int64(msgID), nil
				}
			}
		}
	}

	if msgMap, ok := response.(map[string]interface{}); ok {
		if msgID, ok := msgMap["message_id"].(float64); ok {
			return int64(msgID), nil
		}
	}

	return 0, fmt.Errorf("unexpected response format: %v", response)
}

func (c *BotClient) SendMessage(peerID int64, text string) (int64, error) {
	text = internalmsg.Strip(text)
	if text == "" {
		return 0, fmt.Errorf("empty message text")
	}

	if len(text) > 2000 {
		parts := c.splitText(text, 2000)
		lastMsgID := int64(0)

		for i, part := range parts {
			partText := fmt.Sprintf("[%d/%d]\n%s", i+1, len(parts), part)
			msgID, err := c.sendSingleMessage(peerID, partText, "", nil)
			if err != nil {
				continue
			}
			lastMsgID = msgID

			if i < len(parts)-1 {
				time.Sleep(300 * time.Millisecond)
			}
		}
		return lastMsgID, nil
	}

	return c.sendSingleMessage(peerID, text, "", nil)
}

func (c *BotClient) SendMessageWithKeyboard(peerID int64, text string, keyboard map[string]interface{}) (int64, error) {
	return c.sendSingleMessage(peerID, text, "", keyboard)
}

func (c *BotClient) sendSingleMessage(peerID int64, text, attachment string, keyboard map[string]interface{}) (int64, error) {
	stripped := internalmsg.Strip(text)
	if stripped != text {
		if stripped == "" && attachment == "" {
			return 0, fmt.Errorf("empty message text")
		}
		text = stripped
	}

	params := map[string]interface{}{
		"peer_id":      peerID,
		"random_id":    time.Now().UnixMilli(),
		"v":            c.apiVersion,
		"access_token": c.token,
	}
	if text != "" {
		params["message"] = text
	}
	if attachment != "" {
		params["attachment"] = attachment
	}
	if keyboard != nil {
		kbJSON, _ := json.Marshal(keyboard)
		params["keyboard"] = string(kbJSON)
	}

	responseBody, err := c.doRequestPOST("messages.send", params)
	if err != nil {
		return 0, err
	}

	var fullResponse struct {
		Response interface{} `json:"response"`
	}
	if err := json.Unmarshal(responseBody, &fullResponse); err != nil {
		return 0, fmt.Errorf("failed to parse response: %w", err)
	}

	return extractMessageID(fullResponse.Response)
}

func (c *BotClient) EditMessage(peerID, messageID int64, text string, keyboard map[string]interface{}) error {
	params := map[string]interface{}{
		"peer_id":      peerID,
		"message_id":   messageID,
		"message":      text,
		"v":            c.apiVersion,
		"access_token": c.token,
	}
	if keyboard != nil {
		kbJSON, _ := json.Marshal(keyboard)
		params["keyboard"] = string(kbJSON)
	}

	_, err := c.doRequestPOST("messages.edit", params)
	return err
}

func (c *BotClient) SendMessageEventAnswer(eventID string, userID, peerID int64, text string) error {
	params := map[string]interface{}{
		"event_id":     eventID,
		"user_id":      userID,
		"peer_id":      peerID,
		"v":            c.apiVersion,
		"access_token": c.token,
	}
	if text != "" {
		params["text"] = text
	}

	_, err := c.doRequestPOST("messages.sendMessageEventAnswer", params)
	return err
}

func (c *BotClient) GetMessagesByID(messageIDs []int64) ([]VKMessage, error) {
	idsStr := ""
	for i, id := range messageIDs {
		if i > 0 {
			idsStr += ","
		}
		idsStr += fmt.Sprintf("%d", id)
	}

	logger.DebugToFile("[vk] messages.getById request: ids=%s", idsStr)

	params := map[string]interface{}{
		"message_ids": idsStr,
	}

	responseBody, err := c.doRequestGET("messages.getById", params)
	if err != nil {
		logger.DebugToFile("[vk] messages.getById failed: ids=%s err=%v", idsStr, err)
		return nil, err
	}

	var response struct {
		Response struct {
			Items []VKMessage `json:"items"`
		} `json:"response"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		logger.DebugToFile("[vk] messages.getById: parse error: ids=%s body=%s", idsStr, truncateForLog(responseBody, 300))
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	logger.DebugToFile("[vk] messages.getById ok: ids=%s items=%d", idsStr, len(response.Response.Items))
	return response.Response.Items, nil
}

func truncateForLog(data []byte, max int) string {
	if len(data) <= max {
		return string(data)
	}
	return string(data[:max]) + "..."
}

func (c *BotClient) GetBestVideoURL(ownerID, videoID int64) (string, error) {
	params := map[string]interface{}{
		"owner_id": ownerID,
		"video_id": videoID,
	}
	responseBody, err := c.doRequestGET("videos.get", params)
	if err != nil {
		return "", fmt.Errorf("videos.get: %w", err)
	}

	var response struct {
		Response []struct {
			Files map[string]interface{} `json:"files"`
		} `json:"response"`
	}
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", fmt.Errorf("parse videos.get response: %w", err)
	}
	if len(response.Response) == 0 {
		return "", nil
	}
	return findBestMp4(response.Response[0].Files), nil
}

func (c *BotClient) splitText(text string, maxLength int) []string {
	safeLength := maxLength - 20

	parts := []string{}
	currentPart := ""
	lines := splitLines(text)

	for _, line := range lines {
		if len(line) > safeLength {
			if currentPart != "" {
				parts = append(parts, currentPart)
				currentPart = ""
			}
			for i := 0; i < len(line); i += safeLength {
				end := i + safeLength
				if end > len(line) {
					end = len(line)
				}
				parts = append(parts, line[i:end])
			}
		} else if currentPart != "" && len(currentPart)+len(line)+1 > safeLength {
			parts = append(parts, currentPart)
			currentPart = line
		} else {
			if currentPart != "" {
				currentPart += "\n" + line
			} else {
				currentPart = line
			}
		}
	}

	if currentPart != "" {
		parts = append(parts, currentPart)
	}

	return parts
}

func splitLines(text string) []string {
	lines := []string{}
	currentLine := ""
	for _, char := range text {
		if char == '\n' {
			lines = append(lines, currentLine)
			currentLine = ""
		} else {
			currentLine += string(char)
		}
	}
	if currentLine != "" {
		lines = append(lines, currentLine)
	}
	return lines
}

func (c *BotClient) SendThinking(peerID int64, content string) (int64, error) {
	if content == "" {
		return 0, fmt.Errorf("empty thinking content")
	}

	return c.SendMessage(peerID, SanitizeMentionAll(content))
}

func CreateQuestionKeyboard(header string, questionText string, options []map[string]string) map[string]interface{} {
	buttons := [][]map[string]interface{}{}
	for _, opt := range options {
		row := []map[string]interface{}{
			{
				"action": map[string]interface{}{
					"type":  "text",
					"label": opt["label"],
				},
				"color": "primary",
			},
		}
		buttons = append(buttons, row)
	}

	keyboard := map[string]interface{}{
		"inline":  false,
		"buttons": buttons,
	}

	return keyboard
}

func CreateKeyboard(buttons [][]map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"inline":  false,
		"buttons": buttons,
	}
}

func CreateCommandKeyboard() map[string]interface{} {
	return map[string]interface{}{
		"inline": false,
		"buttons": [][]map[string]interface{}{
			{
				{"action": map[string]interface{}{"type": "text", "label": "/status"}, "color": "secondary"},
				{"action": map[string]interface{}{"type": "text", "label": "/clear"}, "color": "negative"},
			},
			{
				{"action": map[string]interface{}{"type": "text", "label": "/abort"}, "color": "negative"},
				{"action": map[string]interface{}{"type": "text", "label": "/restart"}, "color": "primary"},
			},
		},
	}
}

// CreateModelsKeyboard builds an inline keyboard of model buttons. Each
// button carries the "provider/id" reference in its callback payload; the
// handler maps it to /r <ref>.
func CreateModelsKeyboard(models []string, currentRef string) map[string]interface{} {
	const buttonsPerRow = 2
	var rows [][]map[string]interface{}
	for i := 0; i < len(models); i += buttonsPerRow {
		end := i + buttonsPerRow
		if end > len(models) {
			end = len(models)
		}
		row := make([]map[string]interface{}, 0, end-i)
		for _, ref := range models[i:end] {
			color := "secondary"
			if ref == currentRef {
				color = "positive"
			}
			payloadJSON, _ := json.Marshal(map[string]string{
				"command": "model_switch",
				"ref":     ref,
			})
			row = append(row, map[string]interface{}{
				"action": map[string]interface{}{
					"type":    "callback",
					"label":   ref,
					"payload": string(payloadJSON),
				},
				"color": color,
			})
		}
		rows = append(rows, row)
	}
	return map[string]interface{}{
		"inline":  true,
		"buttons": rows,
	}
}
