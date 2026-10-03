package http

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

func transportLoggingMiddleware(log logging.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		started := time.Now()
		err := c.Next()
		fields := []logging.Field{logging.String("http_method", c.Method()), logging.String("http_path", c.Path()), logging.Int("http_status", c.Response().StatusCode()), logging.DurationMS(time.Since(started))}
		if logging.IsVerbose(log) {
			fields = append(fields, logging.Int("request_bytes", len(c.Body())), logging.Int("response_bytes", len(c.Response().Body())))
		}
		if err != nil {
			fields = append(fields, logging.Err(err))
			log.Warn("http request completed", fields...)
		} else {
			log.Info("http request completed", fields...)
		}
		return err
	}
}
