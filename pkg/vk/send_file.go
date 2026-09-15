package vk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var vkBlockedExtensions = map[string]bool{
	"html": true, "htm": true, "svg": true, "js": true, "mjs": true,
	"php": true, "asp": true, "aspx": true, "jsp": true, "exe": true,
	"bat": true, "cmd": true, "sh": true, "py": true,
}

func SafeUploadName(filename string) string {
	if filename == "" {
		return filename
	}
	ext := strings.ToLower(filepath.Ext(filename))
	ext = strings.TrimPrefix(ext, ".")
	if vkBlockedExtensions[ext] {
		return filename + ".txt"
	}
	return filename
}

func (c *BotClient) UploadAndSendDocument(filePath string, peerID int64, message string) (int64, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return 0, fmt.Errorf("file not found: %s", filePath)
	}

	filename := filepath.Base(filePath)
	safeName := SafeUploadName(filename)

	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("read file: %w", err)
	}

	fileField, err := c.uploadDoc(peerID, fileData, safeName)
	if err != nil {
		return 0, err
	}

	ownerID, docID, err := c.saveDoc(fileField, filename)
	if err != nil {
		return 0, err
	}

	attachment := fmt.Sprintf("doc%d_%d", ownerID, docID)
	return c.sendMessageWithAttachment(peerID, attachment, message)
}

func (c *BotClient) getDocUploadURL(peerID int64) (string, error) {
	resp, err := c.doRequestGET("docs.getMessagesUploadServer", map[string]interface{}{
		"type":    "doc",
		"peer_id": peerID,
	})
	if err != nil {
		return "", fmt.Errorf("docs.getMessagesUploadServer: %w", err)
	}

	var out struct {
		Response struct {
			UploadURL string `json:"upload_url"`
		} `json:"response"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", fmt.Errorf("parse upload server response: %w", err)
	}
	if out.Response.UploadURL == "" {
		return "", fmt.Errorf("no upload_url in response")
	}
	return out.Response.UploadURL, nil
}

func (c *BotClient) uploadDoc(peerID int64, fileData []byte, filename string) (string, error) {
	uploadURL, err := c.getDocUploadURL(peerID)
	if err != nil {
		return "", err
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		fileField, retryable, err := c.tryUploadOnce(uploadURL, fileData, filename)
		if err == nil {
			return fileField, nil
		}
		lastErr = err
		if !retryable || attempt == maxAttempts {
			break
		}

		if fresh, uerr := c.getDocUploadURL(peerID); uerr == nil {
			uploadURL = fresh
		}
	}
	return "", lastErr
}

func (c *BotClient) tryUploadOnce(uploadURL string, fileData []byte, filename string) (string, bool, error) {
	boundary := fmt.Sprintf("----WebKitFormBoundary%d", rand.Int63n(1000000000000))
	var buf bytes.Buffer
	buf.WriteString("--" + boundary + "\r\n")
	buf.WriteString(fmt.Sprintf("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n", filename))
	buf.WriteString("Content-Type: application/octet-stream\r\n")
	buf.WriteString("\r\n")
	buf.Write(fileData)
	buf.WriteString("\r\n--" + boundary + "--\r\n")

	req, err := http.NewRequest("POST", uploadURL, &buf)
	if err != nil {
		return "", false, fmt.Errorf("create upload request: %w", err)
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("upload request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("read upload response: %w", err)
	}
	if resp.StatusCode == http.StatusMethodNotAllowed {

		return "", true, fmt.Errorf("upload rejected (405): %s", truncateBody(body))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("upload failed: HTTP %d - %s", resp.StatusCode, truncateBody(body))
	}

	var out struct {
		File    string `json:"file"`
		Error   string `json:"error"`
		ErrDesc string `json:"error_descr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", false, fmt.Errorf("parse upload response: %w", err)
	}
	if out.File != "" {
		return out.File, false, nil
	}

	if out.Error == "no_file" || out.Error == "wrong_file" {
		return "", true, fmt.Errorf("upload rejected by VK (%s): %s", out.Error, truncateBody(body))
	}
	return "", false, fmt.Errorf("no file field in upload response: %s", truncateBody(body))
}

func (c *BotClient) saveDoc(fileField, title string) (ownerID, docID int64, err error) {
	resp, err := c.doRequestPOST("docs.save", map[string]interface{}{
		"file":         fileField,
		"title":        title,
		"v":            c.apiVersion,
		"access_token": c.token,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("docs.save: %w", err)
	}

	var out struct {
		Response struct {
			Doc struct {
				ID      int64 `json:"id"`
				OwnerID int64 `json:"owner_id"`
			} `json:"doc"`
		} `json:"response"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, 0, fmt.Errorf("parse docs.save response: %w", err)
	}
	if out.Response.Doc.ID == 0 {
		return 0, 0, fmt.Errorf("empty docs.save response: %s", truncateBody(resp))
	}
	return out.Response.Doc.OwnerID, out.Response.Doc.ID, nil
}

func (c *BotClient) sendMessageWithAttachment(peerID int64, attachment, message string) (int64, error) {
	params := map[string]interface{}{
		"peer_id":      peerID,
		"attachment":   attachment,
		"random_id":    time.Now().UnixMilli(),
		"v":            c.apiVersion,
		"access_token": c.token,
	}
	if message != "" {
		params["message"] = message
	}

	resp, err := c.doRequestPOST("messages.send", params)
	if err != nil {
		return 0, fmt.Errorf("messages.send: %w", err)
	}

	var out struct {
		Response interface{} `json:"response"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, fmt.Errorf("parse messages.send response: %w", err)
	}
	return extractMessageID(out.Response)
}

func truncateBody(b []byte) string {
	const max = 300
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
