package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Environment string           `yaml:"environment" env:"APP_ENV"`
	Log         LogConfig        `yaml:"log" envPrefix:"LOG_"`
	HTTP        HTTPConfig       `yaml:"http" envPrefix:"HTTP_"`
	GRPC        GRPCConfig       `yaml:"grpc" envPrefix:"GRPC_"`
	Postgres    PostgresConfig   `yaml:"postgres" envPrefix:"POSTGRES_"`
	Redis       RedisConfig      `yaml:"redis" envPrefix:"REDIS_"`
	Auth        AuthConfig       `yaml:"auth" envPrefix:"AUTH_"`
	Scheduler   SchedulerConfig  `yaml:"scheduler" envPrefix:"SCHEDULER_"`
	Worker      WorkerConfig     `yaml:"worker" envPrefix:"WORKER_"`
	Migrations  MigrationsConfig `yaml:"migrations" envPrefix:"MIGRATIONS_"`
}

type LogConfig struct {
	Level  string `yaml:"level" env:"LEVEL"`
	Format string `yaml:"format" env:"FORMAT"`
}

type HTTPConfig struct {
	Addr            string        `yaml:"addr" env:"ADDR"`
	ReadTimeout     time.Duration `yaml:"read_timeout" env:"READ_TIMEOUT"`
	WriteTimeout    time.Duration `yaml:"write_timeout" env:"WRITE_TIMEOUT"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"SHUTDOWN_TIMEOUT"`
}

type GRPCConfig struct {
	Addr string `yaml:"addr" env:"ADDR"`
}

type PostgresConfig struct {
	DSN             string        `yaml:"dsn" env:"DSN"`
	MaxOpenConns    int           `yaml:"max_open_conns" env:"MAX_OPEN_CONNS"`
	MaxIdleConns    int           `yaml:"max_idle_conns" env:"MAX_IDLE_CONNS"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" env:"CONN_MAX_LIFETIME"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" env:"ADDR"`
	Password string `yaml:"password" env:"PASSWORD"`
	DB       int    `yaml:"db" env:"DB"`
	PoolSize int    `yaml:"pool_size" env:"POOL_SIZE"`
}

type AuthConfig struct {
	BootstrapToken string        `yaml:"bootstrap_token" env:"BOOTSTRAP_TOKEN"`
	TokenTTL       time.Duration `yaml:"token_ttl" env:"TOKEN_TTL"`
}

type SchedulerConfig struct {
	Interval time.Duration `yaml:"interval" env:"INTERVAL"`
}

type WorkerConfig struct {
	Count          int           `yaml:"count" env:"COUNT"`
	PollInterval   time.Duration `yaml:"poll_interval" env:"POLL_INTERVAL"`
	LeaseDuration  time.Duration `yaml:"lease_duration" env:"LEASE_DURATION"`
	HeartbeatEvery time.Duration `yaml:"heartbeat_every" env:"HEARTBEAT_EVERY"`
	MaxConcurrent  int           `yaml:"max_concurrent" env:"MAX_CONCURRENT"`
}

type MigrationsConfig struct {
	Dir string `yaml:"dir" env:"DIR"`
}

func Default() Config {
	return Config{
		Environment: "development",
		Log:         LogConfig{Level: "info", Format: "json"},
		HTTP: HTTPConfig{
			Addr:            ":8080",
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    30 * time.Second,
			ShutdownTimeout: 15 * time.Second,
		},
		GRPC: GRPCConfig{Addr: ":9090"},
		Postgres: PostgresConfig{
			DSN:             "postgres://workflow:workflow@localhost:5432/workflow?sslmode=disable",
			MaxOpenConns:    20,
			MaxIdleConns:    5,
			ConnMaxLifetime: 30 * time.Minute,
		},
		Redis:     RedisConfig{Addr: "localhost:6379", DB: 0, PoolSize: 20},
		Auth:      AuthConfig{BootstrapToken: "dev-secret-token", TokenTTL: 24 * time.Hour},
		Scheduler: SchedulerConfig{Interval: 2 * time.Second},
		Worker: WorkerConfig{
			Count:          2,
			PollInterval:   500 * time.Millisecond,
			LeaseDuration:  30 * time.Second,
			HeartbeatEvery: 5 * time.Second,
			MaxConcurrent:  20,
		},
		Migrations: MigrationsConfig{Dir: "migrations"},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if err := applyEnv(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func applyEnv(cfg *Config) error {
	return applyEnvToStruct("", cfg)
}

func applyEnvToStruct(prefix string, target any) error {
	// The config loader keeps the env surface small and explicit.
	// Duration fields are read from their corresponding *_MS or Go duration string.
	cfg, ok := target.(*Config)
	if !ok {
		return errors.New("unsupported env target")
	}
	cfg.HTTP.Addr = envString("HTTP_ADDR", cfg.HTTP.Addr)
	cfg.GRPC.Addr = envString("GRPC_ADDR", cfg.GRPC.Addr)
	cfg.Postgres.DSN = envString("POSTGRES_DSN", cfg.Postgres.DSN)
	cfg.Redis.Addr = envString("REDIS_ADDR", cfg.Redis.Addr)
	cfg.Redis.Password = envString("REDIS_PASSWORD", cfg.Redis.Password)
	cfg.Auth.BootstrapToken = envString("AUTH_BOOTSTRAP_TOKEN", cfg.Auth.BootstrapToken)
	cfg.Log.Level = envString("LOG_LEVEL", cfg.Log.Level)
	cfg.Log.Format = envString("LOG_FORMAT", cfg.Log.Format)
	cfg.Worker.Count = envInt("WORKER_COUNT", cfg.Worker.Count)
	cfg.Worker.PollInterval = envDuration("WORKER_POLL_INTERVAL", cfg.Worker.PollInterval)
	cfg.Worker.LeaseDuration = envDuration("WORKER_LEASE_DURATION", cfg.Worker.LeaseDuration)
	cfg.Scheduler.Interval = envDuration("SCHEDULER_INTERVAL", cfg.Scheduler.Interval)
	return nil
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
