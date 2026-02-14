package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/david/grant-finder/internal/models"
)

var (
	ErrUserExists   = errors.New("user already exists")
	ErrInvalidCreds = errors.New("invalid credentials")

	jwtSecretOnce    sync.Once
	jwtSecretRuntime []byte
	jwtSecretErr     error
)

func jwtSecretFromEnv() ([]byte, error) {
	jwtSecretOnce.Do(func() {
		secret := strings.TrimSpace(os.Getenv("JWT_SECRET"))
		if secret != "" {
			jwtSecretRuntime = []byte(secret)
			return
		}

		buf := make([]byte, 48)
		if _, err := rand.Read(buf); err != nil {
			jwtSecretErr = fmt.Errorf("failed to generate JWT fallback secret: %w", err)
			return
		}

		jwtSecretRuntime = []byte(base64.RawURLEncoding.EncodeToString(buf))
		log.Print("JWT_SECRET is not set; using ephemeral in-memory fallback secret")
	})

	if jwtSecretErr != nil {
		return nil, jwtSecretErr
	}
	if len(jwtSecretRuntime) == 0 {
		return nil, errors.New("JWT secret unavailable")
	}

	return jwtSecretRuntime, nil
}

type Service struct {
	db *pgxpool.Pool
}

func NewService(db *pgxpool.Pool) *Service {
	return &Service{db: db}
}

func (s *Service) Signup(ctx context.Context, req SignupRequest) (*AuthResponse, error) {
	// check if user exists
	var exists bool
	err := s.db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", req.Email).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrUserExists
	}

	// hash password
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hashing failed: %w", err)
	}

	// insert user
	var user User
	err = s.db.QueryRow(ctx, `
		INSERT INTO users (email, password_hash)
		VALUES ($1, $2)
		RETURNING id, email, created_at
	`, req.Email, string(hash)).Scan(&user.ID, &user.Email, &user.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// generate token
	token, err := generateToken(user.ID)
	if err != nil {
		return nil, err
	}

	return &AuthResponse{Token: token, User: user}, nil
}

func (s *Service) Login(ctx context.Context, req LoginRequest) (*AuthResponse, error) {
	var user User
	err := s.db.QueryRow(ctx, "SELECT id, email, password_hash, created_at FROM users WHERE email = $1", req.Email).Scan(
		&user.ID, &user.Email, &user.PasswordHash, &user.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, ErrInvalidCreds
	}
	if err != nil {
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, ErrInvalidCreds
	}

	token, err := generateToken(user.ID)
	if err != nil {
		return nil, err
	}

	// Clear hash before returning
	user.PasswordHash = ""
	return &AuthResponse{Token: token, User: user}, nil
}

func generateToken(userID uuid.UUID) (string, error) {
	secretKey, err := jwtSecretFromEnv()
	if err != nil {
		return "", err
	}

	claims := jwt.MapClaims{
		"sub": userID.String(),
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(24 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secretKey)
}

// Saved Opportunities

func (s *Service) SaveOpportunity(ctx context.Context, userID uuid.UUID, oppID uuid.UUID) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO saved_opportunities (user_id, opportunity_id)
		VALUES ($1, $2)
		ON CONFLICT (user_id, opportunity_id) DO NOTHING
	`, userID, oppID)
	return err
}

func (s *Service) UnsaveOpportunity(ctx context.Context, userID uuid.UUID, oppID uuid.UUID) error {
	_, err := s.db.Exec(ctx, `
		DELETE FROM saved_opportunities
		WHERE user_id = $1 AND opportunity_id = $2
	`, userID, oppID)
	return err
}

func (s *Service) GetSavedOpportunities(ctx context.Context, userID uuid.UUID) ([]models.Opportunity, error) {
	rows, err := s.db.Query(ctx, `
		SELECT o.id, o.title, o.summary, o.source_domain, o.opportunity_number, 
		       o.agency_name, o.agency_code, o.funder_type, o.amount_min, o.amount_max, 
			   o.currency, o.deadline_at, o.open_date, o.is_rolling, o.doc_type, 
			   o.opp_status, o.region, o.country, o.categories, o.eligibility
		FROM opportunities o
		JOIN saved_opportunities so ON o.id = so.opportunity_id
		WHERE so.user_id = $1
		ORDER BY so.saved_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var opps []models.Opportunity
	for rows.Next() {
		var o models.Opportunity
		// We only scan fields we used in query. Make sure they match model struct field count/order?
		// Or just scan into specific fields.
		err := rows.Scan(
			&o.ID, &o.Title, &o.Summary, &o.SourceDomain, &o.OpportunityNumber,
			&o.AgencyName, &o.AgencyCode, &o.FunderType, &o.AmountMin, &o.AmountMax,
			&o.Currency, &o.DeadlineAt, &o.OpenDate, &o.IsRolling, &o.DocType,
			&o.OppStatus, &o.Region, &o.Country, &o.Categories, &o.Eligibility,
		)
		if err != nil {
			return nil, err
		}
		opps = append(opps, o)
	}
	return opps, nil
}
