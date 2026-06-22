package modelrouter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var defaultFailoverStatusCodes = []int{401, 403, 408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

var requestSkipHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
	"host":                true,
	"content-length":      true,
}

var responseSkipHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
	"host":                true,
}

type Config struct {
	ProxyAPIKeys         []string   `json:"proxyApiKeys"`
	CompactFallbackModel string     `json:"compactFallbackModel"`
	ProxiedPathPrefixes  []string   `json:"proxiedPathPrefixes"`
	FailoverStatusCodes  []int      `json:"failoverStatusCodes"`
	Upstreams            []Upstream `json:"upstreams"`
}

type Upstream struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	BaseURL        string   `json:"baseUrl"`
	APIKey         string   `json:"apiKey"`
	AuthType       string   `json:"authType"`
	Models         []string `json:"models"`
	Enabled        bool     `json:"enabled"`
	Priority       int      `json:"priority"`
	TimeoutSeconds int      `json:"timeoutSeconds"`
}

type State struct {
	ActiveByModel map[string]string  `json:"activeByModel"`
	LastFailures  map[string]Failure `json:"lastFailures"`
}

type Failure struct {
	At     string `json:"at"`
	Detail string `json:"detail"`
}

type Handler struct {
	configPath string
	statePath  string
	logPath    string
	configMu   sync.RWMutex
	stateMu    sync.Mutex
	client     *http.Client
}

func New(configPath, statePath, logPath string) (*Handler, error) {
	h := &Handler{
		configPath: configPath,
		statePath:  statePath,
		logPath:    logPath,
		client:     &http.Client{},
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return nil, err
	}
	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		if err := writeJSONFile(configPath, defaultConfig()); err != nil {
			return nil, err
		}
	}
	h.logf("model router initialized config=%s state=%s", configPath, statePath)
	return h, nil
}

func DefaultPaths(configDir string) (string, string, string) {
	dataDir := filepath.Join(configDir, ".vllm-sr", "model-router")
	configPath := getenv("MODEL_ROUTER_CONFIG", filepath.Join(dataDir, "config.json"))
	statePath := getenv("MODEL_ROUTER_STATE", filepath.Join(dataDir, "state.json"))
	logPath := getenv("MODEL_ROUTER_LOG", filepath.Join(dataDir, "router.log"))
	return configPath, statePath, logPath
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/model-router/config", h.configAPI)
	mux.HandleFunc("/api/model-router/status", h.statusAPI)
	mux.HandleFunc("/api/model-router/logs", h.logsAPI)
	for _, prefix := range []string{"/v1", "/openai", "/anthropic", "/claude"} {
		mux.HandleFunc(prefix, h.Proxy)
		mux.HandleFunc(prefix+"/", h.Proxy)
	}
}

func (h *Handler) Proxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	cfg := h.loadConfig()
	if !isProxiedPath(r.URL.Path, cfg) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
		return
	}
	if !h.proxyAuthOK(r, cfg) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid_proxy_api_key"})
		return
	}
	if r.Method == http.MethodGet && strings.TrimRight(r.URL.Path, "/") == "/v1/models" {
		writeJSON(w, http.StatusOK, syntheticModelsResponse(cfg))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "read_request_failed", "detail": err.Error()})
		return
	}
	model := requestModelFromBody(body)
	compactEndpoint := isCompactRequest(r.URL.Path)
	responsesCompaction := isResponsesCompactionRequest(r.URL.Path, body, cfg)
	compactLike := compactEndpoint || responsesCompaction
	upstreamPath := r.URL.RequestURI()
	if compactEndpoint {
		upstreamPath = compactFallbackPath(r.URL)
	}
	upstreamBody := body
	if compactLike {
		upstreamBody = prepareCompactFallbackBody(body, cfg)
		model = cfg.CompactFallbackModel
	}
	h.logClassification(r.URL.Path, body, model, cfg, compactEndpoint, responsesCompaction, compactLike)

	candidates := h.candidateUpstreams(cfg, model)
	if len(candidates) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_upstream_for_model", "model": model})
		return
	}

	failoverCodes := make(map[int]bool)
	for _, code := range cfg.FailoverStatusCodes {
		failoverCodes[code] = true
	}

	var lastErr error
	for attempt, upstream := range candidates {
		targetURL, err := buildTargetURL(upstream.BaseURL, upstreamPath)
		if err != nil {
			lastErr = err
			h.rememberFailure(upstream.ID, model, err.Error())
			continue
		}
		resp, err := h.doUpstream(r, upstream, targetURL, upstreamBody)
		if err != nil {
			lastErr = err
			h.rememberFailure(upstream.ID, model, err.Error())
			h.logf("FAILOVER_ERROR model=%s upstream=%s %s %s -> %s %v %dms", dash(model), upstream.ID, r.Method, r.URL.Path, targetURL, err, time.Since(started).Milliseconds())
			continue
		}
		if failoverCodes[resp.StatusCode] && attempt < len(candidates)-1 {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			h.rememberFailure(upstream.ID, model, fmt.Sprintf("status %d", resp.StatusCode))
			h.logf("FAILOVER model=%s upstream=%s %s %s -> %s %d %dms", dash(model), upstream.ID, r.Method, r.URL.Path, targetURL, resp.StatusCode, time.Since(started).Milliseconds())
			continue
		}

		if compactLike && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			upstreamRespBody, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": "compact_response_read_failed", "detail": readErr.Error()})
				return
			}
			writeRawJSON(w, http.StatusOK, buildCompactionPayload(upstreamRespBody))
			label := "COMPACT_FALLBACK_RESPONSES"
			if compactEndpoint {
				label = "COMPACT_FALLBACK"
			}
			h.promoteUpstream(model, upstream.ID)
			h.logf("%s model=%s upstream=%s %s %s -> %s 200 %dms", label, dash(model), upstream.ID, r.Method, r.URL.Path, targetURL, time.Since(started).Milliseconds())
			return
		}

		copyResponse(w, resp)
		resp.Body.Close()
		if !failoverCodes[resp.StatusCode] {
			h.promoteUpstream(model, upstream.ID)
		}
		h.logf("OK model=%s upstream=%s %s %s -> %s %d %dms", dash(model), upstream.ID, r.Method, r.URL.Path, targetURL, resp.StatusCode, time.Since(started).Milliseconds())
		return
	}

	writeJSON(w, http.StatusBadGateway, map[string]any{"error": "model_router_failed", "detail": fmt.Sprint(lastErr)})
}

func (h *Handler) configAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, sanitizeConfig(h.loadConfig()))
	case http.MethodPost, http.MethodPut:
		var incoming Config
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_config", "detail": err.Error()})
			return
		}
		existing := h.loadConfig()
		normalized := normalizeConfig(incoming, existing)
		if err := h.saveConfig(normalized); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "save_config_failed", "detail": err.Error()})
			return
		}
		h.logf("CONFIG updated from dashboard API")
		writeJSON(w, http.StatusOK, sanitizeConfig(normalized))
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) statusAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"state":      h.loadState(),
		"log":        h.tailLog(80),
		"configPath": h.configPath,
		"statePath":  h.statePath,
		"logFile":    h.logPath,
	})
}

func (h *Handler) logsAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 500
		if raw := r.URL.Query().Get("lines"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		lines := h.tailLog(limit)
		writeJSON(w, http.StatusOK, map[string]any{
			"logFile": h.logPath,
			"lines":   lines,
			"text":    strings.Join(lines, "\n"),
		})
	case http.MethodDelete:
		if err := os.MkdirAll(filepath.Dir(h.logPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "clear_log_failed", "detail": err.Error()})
			return
		}
		if err := os.WriteFile(h.logPath, nil, 0o644); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "clear_log_failed", "detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "logFile": h.logPath})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func defaultConfig() Config {
	return Config{
		CompactFallbackModel: "gpt-5.4-mini",
		ProxiedPathPrefixes:  []string{"/v1", "/openai", "/anthropic", "/claude"},
		FailoverStatusCodes:  append([]int(nil), defaultFailoverStatusCodes...),
		Upstreams:            []Upstream{},
	}
}

func normalizeConfig(incoming, existing Config) Config {
	result := Config{
		ProxyAPIKeys:         cleanStrings(incoming.ProxyAPIKeys),
		CompactFallbackModel: strings.TrimSpace(incoming.CompactFallbackModel),
		ProxiedPathPrefixes:  normalizePathPrefixes(incoming.ProxiedPathPrefixes),
		FailoverStatusCodes:  normalizeStatusCodes(incoming.FailoverStatusCodes),
	}
	if result.CompactFallbackModel == "" {
		result.CompactFallbackModel = "gpt-5.4-mini"
	}
	existingByID := map[string]Upstream{}
	for _, upstream := range existing.Upstreams {
		existingByID[upstream.ID] = upstream
	}
	for i, upstream := range incoming.Upstreams {
		upstream.ID = firstNonEmpty(strings.TrimSpace(upstream.ID), fmt.Sprintf("upstream-%d", i+1))
		upstream.Name = firstNonEmpty(strings.TrimSpace(upstream.Name), upstream.ID)
		upstream.BaseURL = strings.TrimRight(strings.TrimSpace(upstream.BaseURL), "/")
		if upstream.BaseURL == "" {
			continue
		}
		if upstream.APIKey == "" {
			if existing, ok := existingByID[upstream.ID]; ok {
				upstream.APIKey = existing.APIKey
			}
		}
		upstream.AuthType = normalizeAuthType(upstream.AuthType)
		upstream.Models = normalizeModels(upstream.Models)
		if upstream.TimeoutSeconds < 5 {
			upstream.TimeoutSeconds = 600
		}
		result.Upstreams = append(result.Upstreams, upstream)
	}
	return result
}

func normalizeAuthType(authType string) string {
	switch strings.ToLower(strings.TrimSpace(authType)) {
	case "bearer", "x-api-key", "passthrough", "none":
		return strings.ToLower(strings.TrimSpace(authType))
	default:
		return "bearer"
	}
}

func normalizeModels(models []string) []string {
	clean := cleanStrings(models)
	if len(clean) == 0 {
		return []string{"*"}
	}
	return clean
}

func normalizePathPrefixes(prefixes []string) []string {
	clean := []string{}
	for _, prefix := range prefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" {
			continue
		}
		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}
		prefix = strings.TrimRight(prefix, "/")
		if prefix == "" {
			prefix = "/"
		}
		if !contains(clean, prefix) {
			clean = append(clean, prefix)
		}
	}
	if len(clean) == 0 {
		return []string{"/v1"}
	}
	return clean
}

func normalizeStatusCodes(codes []int) []int {
	clean := []int{}
	for _, code := range codes {
		if code >= 100 && code <= 599 && !containsInt(clean, code) {
			clean = append(clean, code)
		}
	}
	if len(clean) == 0 {
		clean = append(clean, defaultFailoverStatusCodes...)
	}
	sort.Ints(clean)
	return clean
}

func sanitizeConfig(cfg Config) map[string]any {
	upstreams := []map[string]any{}
	for _, upstream := range cfg.Upstreams {
		upstreams = append(upstreams, map[string]any{
			"id":             upstream.ID,
			"name":           upstream.Name,
			"baseUrl":        upstream.BaseURL,
			"apiKey":         "",
			"apiKeySet":      upstream.APIKey != "",
			"authType":       upstream.AuthType,
			"models":         upstream.Models,
			"enabled":        upstream.Enabled,
			"priority":       upstream.Priority,
			"timeoutSeconds": upstream.TimeoutSeconds,
		})
	}
	return map[string]any{
		"proxyApiKeys":         cfg.ProxyAPIKeys,
		"compactFallbackModel": cfg.CompactFallbackModel,
		"proxiedPathPrefixes":  cfg.ProxiedPathPrefixes,
		"failoverStatusCodes":  cfg.FailoverStatusCodes,
		"upstreams":            upstreams,
	}
}

func (h *Handler) loadConfig() Config {
	h.configMu.RLock()
	defer h.configMu.RUnlock()
	var cfg Config
	if err := readJSONFile(h.configPath, &cfg); err != nil {
		return defaultConfig()
	}
	return normalizeConfig(cfg, cfg)
}

func (h *Handler) saveConfig(cfg Config) error {
	h.configMu.Lock()
	defer h.configMu.Unlock()
	return writeJSONFile(h.configPath, cfg)
}

func (h *Handler) loadState() State {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	var state State
	if err := readJSONFile(h.statePath, &state); err != nil {
		return State{ActiveByModel: map[string]string{}, LastFailures: map[string]Failure{}}
	}
	if state.ActiveByModel == nil {
		state.ActiveByModel = map[string]string{}
	}
	if state.LastFailures == nil {
		state.LastFailures = map[string]Failure{}
	}
	return state
}

func (h *Handler) saveState(state State) {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	if state.ActiveByModel == nil {
		state.ActiveByModel = map[string]string{}
	}
	if state.LastFailures == nil {
		state.LastFailures = map[string]Failure{}
	}
	if err := writeJSONFile(h.statePath, state); err != nil {
		log.Printf("model router state write failed: %v", err)
	}
}

func (h *Handler) rememberFailure(upstreamID, model, detail string) {
	state := h.loadState()
	key := stateModelKey(model) + ":" + upstreamID
	if len(detail) > 500 {
		detail = detail[:500]
	}
	state.LastFailures[key] = Failure{At: nowISO(), Detail: detail}
	h.saveState(state)
}

func (h *Handler) promoteUpstream(model, upstreamID string) {
	state := h.loadState()
	state.ActiveByModel[stateModelKey(model)] = upstreamID
	h.saveState(state)
}

func (h *Handler) candidateUpstreams(cfg Config, model string) []Upstream {
	enabled := []Upstream{}
	for _, upstream := range cfg.Upstreams {
		if upstream.Enabled && supportsModel(upstream, model) {
			enabled = append(enabled, upstream)
		}
	}
	sort.SliceStable(enabled, func(i, j int) bool {
		if enabled[i].Priority == enabled[j].Priority {
			return enabled[i].ID < enabled[j].ID
		}
		return enabled[i].Priority < enabled[j].Priority
	})
	activeID := h.loadState().ActiveByModel[stateModelKey(model)]
	if activeID == "" {
		return enabled
	}
	active := []Upstream{}
	rest := []Upstream{}
	for _, upstream := range enabled {
		if upstream.ID == activeID {
			active = append(active, upstream)
		} else {
			rest = append(rest, upstream)
		}
	}
	return append(active, rest...)
}

func (h *Handler) proxyAuthOK(r *http.Request, cfg Config) bool {
	if len(cfg.ProxyAPIKeys) == 0 {
		return true
	}
	candidates := []string{
		extractBearer(r.Header.Get("Authorization")),
		r.Header.Get("x-api-key"),
		r.Header.Get("X-Api-Key"),
	}
	for _, candidate := range candidates {
		for _, key := range cfg.ProxyAPIKeys {
			if constantTimeEqual(candidate, key) {
				return true
			}
		}
	}
	return false
}

func (h *Handler) doUpstream(source *http.Request, upstream Upstream, targetURL string, body []byte) (*http.Response, error) {
	ctx := source.Context()
	if upstream.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(upstream.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, source.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for key, values := range source.Header {
		if requestSkipHeaders[strings.ToLower(key)] {
			continue
		}
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	applyUpstreamAuth(req.Header, upstream)
	return h.client.Do(req)
}

func applyUpstreamAuth(headers http.Header, upstream Upstream) {
	if upstream.AuthType == "passthrough" {
		return
	}
	headers.Del("Authorization")
	headers.Del("x-api-key")
	headers.Del("X-Api-Key")
	if upstream.APIKey == "" || upstream.AuthType == "none" {
		return
	}
	if upstream.AuthType == "x-api-key" {
		headers.Set("x-api-key", upstream.APIKey)
	} else {
		headers.Set("Authorization", "Bearer "+upstream.APIKey)
	}
}

func requestModelFromBody(body []byte) string {
	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	for _, key := range []string{"model", "target_model", "model_id", "deployment"} {
		if value, ok := payload[key].(string); ok {
			return value
		}
	}
	return ""
}

func isProxiedPath(path string, cfg Config) bool {
	for _, prefix := range cfg.ProxiedPathPrefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func supportsModel(upstream Upstream, model string) bool {
	for _, supported := range upstream.Models {
		if supported == "*" || model == "" || supported == model {
			return true
		}
	}
	return false
}

func buildTargetURL(baseURL, requestURI string) (string, error) {
	base, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "", err
	}
	incoming, err := url.ParseRequestURI(requestURI)
	if err != nil {
		return "", err
	}
	suffix := incoming.Path
	basePath := strings.TrimRight(base.Path, "/")
	if strings.HasSuffix(basePath, "/v1") && suffix == "/v1" {
		suffix = ""
	} else if strings.HasSuffix(basePath, "/v1") && strings.HasPrefix(suffix, "/v1/") {
		suffix = strings.TrimPrefix(suffix, "/v1")
	}
	base.Path = basePath + suffix
	base.RawQuery = incoming.RawQuery
	return base.String(), nil
}

func isCompactRequest(path string) bool {
	return strings.HasSuffix(strings.TrimRight(path, "/"), "/responses/compact")
}

func isResponsesRequest(path string) bool {
	return strings.HasSuffix(strings.TrimRight(path, "/"), "/responses")
}

type compactionInfo struct {
	CompactEndpoint     bool
	ResponsesEndpoint   bool
	Model               string
	CompactModel        string
	ModelMatchesCompact bool
	ControlSignal       bool
	InputSignal         bool
	LargeBodySignal     bool
}

func compactionSignalInfo(path string, body []byte, cfg Config) compactionInfo {
	payload := map[string]any{}
	info := compactionInfo{
		CompactEndpoint:   isCompactRequest(path),
		ResponsesEndpoint: isResponsesRequest(path),
		CompactModel:      cfg.CompactFallbackModel,
		LargeBodySignal:   len(body) > 50_000,
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return info
	}
	if model, ok := payload["model"].(string); ok {
		info.Model = model
	}
	info.ModelMatchesCompact = info.Model == cfg.CompactFallbackModel
	controlFields := map[string]any{
		"instructions":    payload["instructions"],
		"text":            payload["text"],
		"response_format": payload["response_format"],
		"metadata":        payload["metadata"],
	}
	controlBytes, _ := json.Marshal(controlFields)
	controlSerialized := strings.ToLower(string(controlBytes))
	for _, signal := range []string{"compact", "compaction", "response.compaction", "replacement_history"} {
		if strings.Contains(controlSerialized, signal) {
			info.ControlSignal = true
			break
		}
	}
	allBytes, _ := json.Marshal(payload)
	allSerialized := strings.ToLower(string(allBytes))
	info.InputSignal = strings.Contains(allSerialized, "compaction output") || strings.Contains(allSerialized, "remote compaction")
	return info
}

func isResponsesCompactionRequest(path string, body []byte, cfg Config) bool {
	if isCompactRequest(path) || !isResponsesRequest(path) {
		return false
	}
	info := compactionSignalInfo(path, body, cfg)
	if info.ModelMatchesCompact {
		return info.ControlSignal || info.InputSignal || info.LargeBodySignal
	}
	return info.InputSignal || (info.ControlSignal && info.LargeBodySignal)
}

func compactFallbackPath(u *url.URL) string {
	clone := *u
	clone.Path = strings.TrimRight(clone.Path, "/")
	if strings.HasSuffix(clone.Path, "/responses/compact") {
		clone.Path = strings.TrimSuffix(clone.Path, "/compact")
	}
	return clone.RequestURI()
}

func prepareCompactFallbackBody(body []byte, cfg Config) []byte {
	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}
	payload["model"] = cfg.CompactFallbackModel
	delete(payload, "stream")
	delete(payload, "stream_options")
	payload["store"] = false
	next, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return next
}

func buildCompactionPayload(upstreamBody []byte) []byte {
	var payload map[string]any
	_ = json.Unmarshal(upstreamBody, &payload)
	text := extractOutputText(payload)
	if strings.TrimSpace(text) == "" {
		text = "Compaction completed, but the upstream response did not include output text."
	}
	now := time.Now()
	response := map[string]any{
		"id":         fmt.Sprintf("resp_compact_proxy_%d", now.UnixMilli()),
		"object":     "response.compaction",
		"created_at": now.Unix(),
		"output": []map[string]any{
			{
				"id":                fmt.Sprintf("cmp_proxy_%d", now.UnixMilli()),
				"type":              "compaction",
				"encrypted_content": text,
				"created_by":        "assistant",
			},
		},
	}
	if usage, ok := payload["usage"].(map[string]any); ok {
		response["usage"] = usage
	}
	data, _ := json.Marshal(response)
	return data
}

func extractOutputText(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if value, ok := payload["output_text"].(string); ok && strings.TrimSpace(value) != "" {
		return value
	}
	var chunks []string
	output, _ := payload["output"].([]any)
	for _, rawItem := range output {
		item, _ := rawItem.(map[string]any)
		content, _ := item["content"].([]any)
		for _, rawContent := range content {
			contentItem, _ := rawContent.(map[string]any)
			if text, ok := contentItem["text"].(string); ok {
				chunks = append(chunks, text)
			}
		}
	}
	return strings.TrimSpace(strings.Join(chunks, "\n"))
}

func syntheticModelsResponse(cfg Config) map[string]any {
	seen := map[string]bool{}
	var models []map[string]any
	for _, upstream := range cfg.Upstreams {
		if !upstream.Enabled {
			continue
		}
		for _, model := range upstream.Models {
			if model == "*" || seen[model] {
				continue
			}
			seen[model] = true
			models = append(models, map[string]any{
				"id":       model,
				"object":   "model",
				"created":  0,
				"owned_by": "semantic-router-model-router",
			})
		}
	}
	sort.Slice(models, func(i, j int) bool {
		return fmt.Sprint(models[i]["id"]) < fmt.Sprint(models[j]["id"])
	})
	return map[string]any{"object": "list", "data": models}
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		if responseSkipHeaders[strings.ToLower(key)] {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handler) logClassification(path string, body []byte, model string, cfg Config, compactEndpoint, responsesCompaction, compactLike bool) {
	info := compactionSignalInfo(path, body, cfg)
	bodyHash := sha256.Sum256(body)
	h.logf(
		"CLASSIFY path=%s model=%s body_bytes=%d body_sha256=%x compact_endpoint=%d responses_compact=%d compact_like=%d model_matches_compact=%d control_signal=%d input_signal=%d large_body=%d",
		path,
		dash(model),
		len(body),
		bodyHash[:6],
		boolInt(compactEndpoint),
		boolInt(responsesCompaction),
		boolInt(compactLike),
		boolInt(info.ModelMatchesCompact),
		boolInt(info.ControlSignal),
		boolInt(info.InputSignal),
		boolInt(info.LargeBodySignal),
	)
}

func (h *Handler) logf(format string, args ...any) {
	line := nowISO() + " " + fmt.Sprintf(format, args...) + "\n"
	if err := os.MkdirAll(filepath.Dir(h.logPath), 0o755); err != nil {
		log.Printf("model router log mkdir failed: %v", err)
		return
	}
	f, err := os.OpenFile(h.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("model router log open failed: %v", err)
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

func (h *Handler) tailLog(limit int) []string {
	if limit < 1 {
		limit = 1
	}
	if limit > 5000 {
		limit = 5000
	}
	data, err := os.ReadFile(h.logPath)
	if err != nil {
		return []string{}
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > limit {
		lines = lines[len(lines)-limit:]
	}
	return lines
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeRawJSON(w, status, body)
}

func writeRawJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func readJSONFile(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func writeJSONFile(path string, payload any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(path, body, 0o600)
}

func cleanStrings(values []string) []string {
	clean := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !contains(clean, value) {
			clean = append(clean, value)
		}
	}
	return clean
}

func extractBearer(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return value
}

func constantTimeEqual(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func stateModelKey(model string) string {
	if model == "" {
		return "__default__"
	}
	return model
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
