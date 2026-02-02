package logger

import (
	"os"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger
var globalSugaredLogger *zap.SugaredLogger

// InitLogger initializes the global logger based on LOG_LEVEL environment variable
// Supported levels: debug, info, warn, error (default: info)
// Uses console format for development, JSON for production
func InitLogger() (*zap.Logger, *zap.SugaredLogger) {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	environment := os.Getenv("ENVIRONMENT")
	isDevelopment := environment != "production"

	var zapLevel zapcore.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	var logger *zap.Logger
	var err error

	if isDevelopment {
		// Development mode: pretty console output with colors
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zapLevel)
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		config.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05.000")
		config.EncoderConfig.LineEnding = "\n\n"
		config.EncoderConfig.CallerKey = zapcore.OmitKey
		logger, err = config.Build()
	} else {
		// Production mode: JSON output
		config := zap.Config{
			Level:       zap.NewAtomicLevelAt(zapLevel),
			Development: false,
			Encoding:    "json",
			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:        "timestamp",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				FunctionKey:    zapcore.OmitKey,
				MessageKey:     "message",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.MillisDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			},
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		}
		logger, err = config.Build()
	}

	if err != nil {
		panic(err)
	}

	globalLogger = logger
	globalSugaredLogger = logger.Sugar()

	return logger, globalSugaredLogger
}

// GetLogger returns the global structured logger
func GetLogger() *zap.Logger {
	if globalLogger == nil {
		InitLogger()
	}
	return globalLogger
}

// GetSugaredLogger returns the global sugared logger
func GetSugaredLogger() *zap.SugaredLogger {
	if globalSugaredLogger == nil {
		InitLogger()
	}
	return globalSugaredLogger
}

// GenerateRequestID generates a unique request ID using UUID v4
func GenerateRequestID() string {
	return uuid.New().String()
}

// WithRequestID adds a request_id field to a logger
func WithRequestID(logger *zap.Logger, requestID string) *zap.Logger {
	return logger.With(zap.String("request_id", requestID))
}

// TruncateString truncates a string to maxLen characters with "..." suffix if truncated
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 3 {
		return "..."
	}
	return s[:maxLen-3] + "..."
}

// TruncateEmbedding returns the first N dimensions of an embedding slice for debug logging
func TruncateEmbedding(embedding []float32, maxDims int) []float32 {
	if len(embedding) <= maxDims {
		return embedding
	}
	return embedding[:maxDims]
}
