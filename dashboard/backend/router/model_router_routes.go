package router

import (
	"log"
	"net/http"

	"github.com/vllm-project/semantic-router/dashboard/backend/config"
	"github.com/vllm-project/semantic-router/dashboard/backend/modelrouter"
)

func registerModelRouterRoutes(mux *http.ServeMux, cfg *config.Config) {
	configPath, statePath, logPath := modelrouter.DefaultPaths(cfg.ConfigDir)

	handler, err := modelrouter.New(configPath, statePath, logPath)
	if err != nil {
		log.Printf("Warning: model router unavailable: %v", err)
		return
	}

	handler.Register(mux)
	log.Printf("Model router proxy registered: /v1, /openai, /anthropic, /claude")
	log.Printf("Model router admin API registered: /api/model-router")
}
