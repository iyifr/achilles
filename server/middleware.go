package server

import (
	logger "achillesdb/pkgs/logger"
	"time"

	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

// LoggingMiddleware logs HTTP requests with request IDs, timings, and optional body details
func LoggingMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		requestID := logger.GenerateRequestID()
		startTime := time.Now()

		// Get the global logger and add request_id context
		baseLogger := logger.GetLogger()
		requestLogger := logger.WithRequestID(baseLogger, requestID)
		sugaredRequestLogger := requestLogger.Sugar()

		// Store logger in context for use by handlers
		ctx.SetUserValue("logger", sugaredRequestLogger)

		// Log request start
		method := string(ctx.Method())
		path := string(ctx.Path())

		sugaredRequestLogger.Infow("request_start",
			"method", method,
			"path", path,
			"client_ip", ctx.RemoteIP().String(),
		)

		// Log request body in debug mode
		if isDebugLevel() {
			body := ctx.Request.Body()
			if len(body) > 0 {
				truncatedBody := logger.TruncateString(string(body), 500)
				sugaredRequestLogger.Debugw("request_body", "body", truncatedBody)
			}
		}

		// Call the next handler
		next(ctx)

		// Log request completion
		duration := time.Since(startTime)
		statusCode := ctx.Response.StatusCode()

		sugaredRequestLogger.Infow("request_complete",
			"method", method,
			"path", path,
			"status", statusCode,
			"duration_ms", duration.Milliseconds(),
		)

		// Log response body in debug mode
		if isDebugLevel() {
			respBody := ctx.Response.Body()
			if len(respBody) > 0 {
				truncatedRespBody := logger.TruncateString(string(respBody), 500)
				sugaredRequestLogger.Debugw("response_body", "body", truncatedRespBody)
			}
		}
	}
}

// isDebugLevel checks if the logger is at debug level
func isDebugLevel() bool {
	baseLogger := logger.GetLogger()
	return baseLogger.Check(zap.DebugLevel, "") != nil
}
