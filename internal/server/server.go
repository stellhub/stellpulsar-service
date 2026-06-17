package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/stellhub/stellpulsar-service/internal/limiter"
)

type rateLimiter interface {
	Check(request limiter.CheckRequest) (limiter.Decision, error)
}

type Handler struct {
	limiter rateLimiter
}

func NewHandler(limiter rateLimiter) http.Handler {
	handler := &Handler{limiter: limiter}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", onlyMethod(http.MethodGet, handler.health))
	mux.HandleFunc("/api/stellpulsar/v1/limits/check", onlyMethod(http.MethodPost, handler.check))
	return mux
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "stellpulsar-service",
		"status":  "ok",
	})
}

func (h *Handler) check(w http.ResponseWriter, r *http.Request) {
	var request limiter.CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json")
		return
	}

	decision, err := h.limiter.Check(request)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, limiter.ErrLimitInvalid) || errors.Is(err, limiter.ErrWindowInvalid) {
			status = http.StatusUnprocessableEntity
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, decision)
}

func onlyMethod(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		handler(w, r)
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, statusCode int, code string) {
	writeJSON(w, statusCode, map[string]string{"error": code})
}
