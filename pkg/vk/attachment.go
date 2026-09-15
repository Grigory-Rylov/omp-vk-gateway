package vk

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"omp-vk-gateway/pkg/logger"
)

func GetAttachmentDownloadURL(attachment map[string]interface{}, vkClient *BotClient) (string, string, bool) {
	attType, _ := attachment["type"].(string)
	if attType == "" {
		return "", "", false
	}

	attData, ok := attachment[attType].(map[string]interface{})
	if !ok {
		return "", "", false
	}

	switch attType {
	case "photo":
		return extractPhotoURL(attData)
	case "video":
		return extractVideoURL(attData, vkClient)
	case "doc":
		return extractDocURL(attData)
	case "audio_message":
		return extractAudioMessageURL(attData)
	case "audio":
		return extractAudioURL(attData)
	case "sticker":
		return extractStickerURL(attData)
	default:
		return "", "", false
	}
}

func extractPhotoURL(attData map[string]interface{}) (string, string, bool) {
	sizes, _ := attData["sizes"].([]interface{})
	if len(sizes) == 0 {
		return "", "", false
	}

	sorted := sortPhotoSizes(sizes)
	best, ok := sorted[0].(map[string]interface{})
	if !ok {
		return "", "", false
	}

	url, _ := best["url"].(string)
	id, _ := attData["id"].(float64)
	filename := fmt.Sprintf("photo_%.0f.jpg", id)

	return url, filename, url != ""
}

func sortPhotoSizes(sizes []interface{}) []interface{} {
	priority := map[string]int{"s": 1, "m": 2, "x": 3, "y": 4, "z": 5, "w": 6}

	sort.Slice(sizes, func(i, j int) bool {
		si, _ := sizes[i].(map[string]interface{})
		sj, _ := sizes[j].(map[string]interface{})
		tyi, _ := si["type"].(string)
		tyj, _ := sj["type"].(string)
		return priority[tyi] > priority[tyj]
	})

	return sizes
}

func extractVideoURL(attData map[string]interface{}, vkClient *BotClient) (string, string, bool) {
	rawDump, _ := json.Marshal(attData)
	logger.DebugToFile("[extractVideoURL] RAW: %s", string(rawDump))

	if url, filename, ok := extractInlineVideoURL(attData); ok {
		return url, filename, true
	}

	if vkClient != nil {
		if url, filename, ok := extractAPIVideoURL(attData, vkClient); ok {
			return url, filename, true
		}
	}

	if url := probeVideoExtURL(attData); url != "" {
		return url, buildVideoFilename(attData), true
	}

	logger.DebugToFile("[extractVideoURL] no video URL found")
	return "", "", false
}

var videoExtHostOverride string

const VideoExtProbeMinBytes = 100_000

func ConstructVideoExtURL(attData map[string]interface{}) string {
	ownerID := toInt64(attData["owner_id"])
	videoID := toInt64(attData["id"])
	hash, _ := attData["access_key"].(string)
	host := videoExtHostOverride
	if host == "" {
		host = "https://vk.com"
	}
	return fmt.Sprintf("%s/video_ext.php?oid=%d&id=%d&expire=0&hash=%s&hd=1", host, ownerID, videoID, hash)
}

func probeVideoExtURL(attData map[string]interface{}) string {
	url := ConstructVideoExtURL(attData)
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	ok := resp.StatusCode == http.StatusOK && size >= VideoExtProbeMinBytes
	logger.DebugToFile("[probeVideoExtURL] %s status=%d size=%d adopted=%v", url, resp.StatusCode, size, ok)
	if !ok {
		return ""
	}
	return url
}
func extractInlineVideoURL(attData map[string]interface{}) (string, string, bool) {
	files, _ := attData["files"].(map[string]interface{})
	if files != nil {
		logger.DebugToFile("[extractVideoURL] files keys: %v", getMapKeys(files))
		for _, resolution := range []string{"mp4_854", "mp4_640", "mp4_480", "mp4_360"} {
			if url := videoFileURL(files[resolution]); url != "" {
				return url, buildVideoFilename(attData), true
			}
		}
	}

	playURL, _ := attData["playUrl"].(string)
	if playURL != "" {
		return playURL, buildVideoFilename(attData), true
	}

	return "", "", false
}

func extractAPIVideoURL(attData map[string]interface{}, vkClient *BotClient) (string, string, bool) {
	ownerID := toInt64(attData["owner_id"])
	videoID := toInt64(attData["id"])
	if ownerID == 0 && videoID == 0 {
		logger.DebugToFile("[extractVideoURL] no owner_id/id for API fallback")
		return "", "", false
	}
	logger.DebugToFile("[extractVideoURL] trying videos.get: owner_id=%d video_id=%d", ownerID, videoID)
	url, err := vkClient.GetBestVideoURL(ownerID, videoID)
	if err != nil {
		logger.DebugToFile("[extractVideoURL] videos.get failed: %v", err)
		return "", "", false
	}
	if url == "" {
		logger.DebugToFile("[extractVideoURL] videos.get returned no mp4 url")
		return "", "", false
	}
	return url, buildVideoFilename(attData), true
}

func videoFileURL(v interface{}) string {
	switch fv := v.(type) {
	case map[string]interface{}:
		url, _ := fv["url"].(string)
		return url
	case string:
		return fv
	}
	return ""
}

func buildVideoFilename(attData map[string]interface{}) string {
	id, _ := attData["id"].(float64)
	title, _ := attData["title"].(string)
	if title != "" {
		return sanitizeFilename(title + ".mp4")
	}
	return fmt.Sprintf("video_%.0f.mp4", id)
}

func findBestMp4(files map[string]interface{}) string {
	preferred := []string{"mp4_854", "mp4_640", "mp4_480", "mp4_360"}
	for _, res := range preferred {
		if url := videoFileURL(files[res]); url != "" {
			return url
		}
	}

	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var otherMP4, anyURL string
	for _, k := range keys {
		url := videoFileURL(files[k])
		if url == "" {
			continue
		}
		if strings.HasPrefix(k, "mp4_") {
			if otherMP4 == "" {
				otherMP4 = url
			}
			continue
		}
		if anyURL == "" {
			anyURL = url
		}
	}
	if otherMP4 != "" {
		return otherMP4
	}
	return anyURL
}

func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func extractDocURL(attData map[string]interface{}) (string, string, bool) {
	url, _ := attData["url"].(string)
	title, _ := attData["title"].(string)
	id, _ := attData["id"].(float64)

	filename := title
	if filename == "" {
		filename = fmt.Sprintf("doc_%.0f", id)
	}

	return url, filename, url != ""
}

func extractAudioMessageURL(attData map[string]interface{}) (string, string, bool) {
	url, _ := attData["link_mp3"].(string)
	ext := "mp3"
	if url == "" {
		url, _ = attData["link_ogg"].(string)
		ext = "ogg"
	}

	duration, _ := attData["duration"].(float64)
	filename := fmt.Sprintf("voice_msg_%.0fs.%s", duration, ext)

	return url, filename, url != ""
}

func extractAudioURL(attData map[string]interface{}) (string, string, bool) {
	url, _ := attData["url"].(string)
	if url == "" {
		return "", "", false
	}

	id, _ := attData["id"].(float64)
	artist, _ := attData["artist"].(string)
	title, _ := attData["title"].(string)

	filename := fmt.Sprintf("audio_%.0f.mp3", id)
	if artist != "" && title != "" {
		filename = sanitizeFilename(artist + " - " + title + ".mp3")
	}

	return url, filename, true
}

func extractStickerURL(attData map[string]interface{}) (string, string, bool) {
	images, _ := attData["images"].([]interface{})
	if len(images) == 0 {
		return "", "", false
	}

	last, ok := images[len(images)-1].(map[string]interface{})
	if !ok {
		return "", "", false
	}

	url, _ := last["url"].(string)
	id, _ := attData["id"].(float64)
	filename := fmt.Sprintf("sticker_%.0f.png", id)

	return url, filename, url != ""
}

func DownloadAttachments(attachments []map[string]interface{}, saveDir string, vkClient *BotClient) ([]DownloadedAttachment, error) {
	absDir, err := filepath.Abs(saveDir)
	if err != nil {
		return nil, fmt.Errorf("resolve dir %s: %w", saveDir, err)
	}
	if err := os.MkdirAll(absDir, 0755); err != nil {
		return nil, fmt.Errorf("create dir %s: %w", absDir, err)
	}

	var results []DownloadedAttachment
	timestamp := time.Now().Format("20060102_150405")
	var firstErr error

	for _, att := range attachments {
		url, filename, ok := GetAttachmentDownloadURL(att, vkClient)
		if !ok {
			continue
		}

		attType, _ := att["type"].(string)
		safeName := sanitizeFilename(filename)
		name, ext := splitFileName(safeName)
		timedName := fmt.Sprintf("%s_%s%s", timestamp, name, ext)

		result, err := downloadSingle(url, filepath.Join(absDir, timedName))
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("download %s: %w", filename, err)
			}
			continue
		}
		result.Type = attType
		results = append(results, result)
	}

	return results, firstErr
}

func downloadSingle(urlStr, destPath string) (DownloadedAttachment, error) {
	isVideo := strings.HasSuffix(destPath, ".mp4") || strings.HasSuffix(destPath, ".webm")
	timeout := 30 * time.Second
	if isVideo {
		timeout = 5 * time.Minute
	}

	client := &http.Client{Timeout: timeout}

	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return DownloadedAttachment{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://vk.com/")
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		return DownloadedAttachment{}, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return DownloadedAttachment{}, fmt.Errorf("http error: %d body: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return DownloadedAttachment{}, fmt.Errorf("read body: %w", err)
	}

	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return DownloadedAttachment{}, fmt.Errorf("write file: %w", err)
	}

	return DownloadedAttachment{
		Path:     destPath,
		Filename: filepath.Base(destPath),
	}, nil
}

func splitFileName(filename string) (string, string) {
	idx := strings.LastIndex(filename, ".")
	if idx <= 0 {
		return filename, ""
	}
	return filename[:idx], filename[idx:]
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9._\-]`)
	return re.ReplaceAllString(name, "_")
}

func FormatAttachmentInfo(downloaded []DownloadedAttachment) string {
	if len(downloaded) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📥 Downloaded %d file(s):\n", len(downloaded)))

	for _, d := range downloaded {
		line := fmt.Sprintf("• [%s] `%s` saved to: `%s`\n", d.Type, d.Filename, d.Path)
		sb.WriteString(line)
	}

	return sb.String()
}
