package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type contextKey string

const UserIDKey contextKey = "user_id"

// Middleware validates the JWT token and adds the UserID to the context
func Middleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		authHeader := c.Request().Header.Get("Authorization")
		if authHeader == "" {
			return echo.NewHTTPError(http.StatusUnauthorized, "Missing Authorization header")
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return echo.NewHTTPError(http.StatusUnauthorized, "Invalid Authorization header format")
		}

		secretKey, err := jwtSecretFromEnv()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Server auth configuration error")
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return secretKey, nil
		})

		if err != nil || !token.Valid {
			return echo.NewHTTPError(http.StatusUnauthorized, "Invalid or expired token")
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return echo.NewHTTPError(http.StatusUnauthorized, "Invalid token claims")
		}

		sub, err := claims.GetSubject()
		if err != nil {
			return echo.NewHTTPError(http.StatusUnauthorized, "Invalid token subject")
		}

		userID, err := uuid.Parse(sub)
		if err != nil {
			return echo.NewHTTPError(http.StatusUnauthorized, "Invalid user ID in token")
		}

		// Store userID in Echo context
		c.Set(string(UserIDKey), userID)
		return next(c)
	}
}

// GetUserIDFromContext helper to retrieve the user ID
func GetUserIDFromContext(c echo.Context) (uuid.UUID, error) {
	val := c.Get(string(UserIDKey))
	id, ok := val.(uuid.UUID)
	if !ok {
		return uuid.Nil, errors.New("user ID not found in context")
	}
	return id, nil
}
