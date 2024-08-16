// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for Elastic exporter.
type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"`
	configretry.BackOffConfig      `mapstructure:"retry_on_failure"`
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`

	// Endpoint is the clickhouse endpoint.
	Endpoint string `mapstructure:"endpoint"`
	// Username is the authentication username.
	Username string `mapstructure:"username"`
	// Password is the authentication password.
	Password configopaque.String `mapstructure:"password"`
	// Database is the database name to export.
	Database string `mapstructure:"database"`
	// ConnectionParams is the extra connection parameters with map format. for example compression/dial_timeout
	ConnectionParams map[string]string `mapstructure:"connection_params"`
	// LogsTableName is the table name for logs. default is `otel_logs`.
	LogsTableName string `mapstructure:"logs_table_name"`
	// TracesTableName is the table name for traces. default is `otel_traces`.
	TracesTableName string `mapstructure:"traces_table_name"`
	// MetricsTableName is the table name for metrics. default is `otel_metrics`.
	MetricsTableName string `mapstructure:"metrics_table_name"`
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`
	// TableEngine is the table engine to use. default is `MergeTree()`.
	TableEngine TableEngine `mapstructure:"table_engine"`
	// ClusterName if set will append `ON CLUSTER` with the provided name when creating tables.
	ClusterName string `mapstructure:"cluster_name"`
	// CreateSchema if set to true will run the DDL for creating the database and tables. default is true.
	CreateSchema bool `mapstructure:"create_schema"`
	// Compress controls the compression algorithm. Valid options: `none` (disabled), `zstd`, `lz4` (default), `gzip`, `deflate`, `br`, `true` (lz4).
	Compress string `mapstructure:"compress"`
	// AsyncInsert if true will enable async inserts. Default is `true`.
	// Ignored if async inserts are configured in the `endpoint` or `connection_params`.
	// Async inserts may still be overridden server-side.
	AsyncInsert bool `mapstructure:"async_insert"`
}

// TableEngine defines the ENGINE string value when creating the table.
type TableEngine struct {
	Name   string `mapstructure:"name"`
	Params string `mapstructure:"params"`
}

const defaultDatabase = "default"
const defaultTableEngineName = "MergeTree"

var (
	errConfigNoEndpoint      = errors.New("endpoint must be specified")
	errConfigInvalidEndpoint = errors.New("endpoint must be url format")
)

// Validate the ClickHouse server configuration.
func (cfg *Config) Validate() (err error) {
	if cfg.Endpoint == "" {
		err = errors.Join(err, errConfigNoEndpoint)
	}
	dsn, e := cfg.buildDSN()
	if e != nil {
		err = errors.Join(err, e)
	}

	// Validate DSN with clickhouse driver.
	// Last chance to catch invalid config.
	if _, e := clickhouse.ParseDSN(dsn); e != nil {
		err = errors.Join(err, e)
	}

	return err
}

func (cfg *Config) buildDSN() (string, error) {
	dsnURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return "", fmt.Errorf("%w: %s", errConfigInvalidEndpoint, err.Error())
	}

	queryParams := dsnURL.Query()

	// Add connection params to query params.
	for k, v := range cfg.ConnectionParams {
		queryParams.Set(k, v)
	}

	// Enable TLS if scheme is https. This flag is necessary to support https connections.
	if dsnURL.Scheme == "https" {
		queryParams.Set("secure", "true")
	}

	// Use async_insert from config if not specified in DSN.
	if !queryParams.Has("async_insert") {
		queryParams.Set("async_insert", fmt.Sprintf("%t", cfg.AsyncInsert))
	}

	if !queryParams.Has("compress") && (cfg.Compress == "" || cfg.Compress == "true") {
		queryParams.Set("compress", "lz4")
	} else if !queryParams.Has("compress") {
		queryParams.Set("compress", cfg.Compress)
	}

	// Use database from config if not specified in path, or if config is not default.
	if dsnURL.Path == "" || cfg.Database != defaultDatabase {
		dsnURL.Path = cfg.Database
	}

	// Override username and password if specified in config.
	if cfg.Username != "" {
		dsnURL.User = url.UserPassword(cfg.Username, string(cfg.Password))
	}

	dsnURL.RawQuery = queryParams.Encode()

	return dsnURL.String(), nil
}

func (cfg *Config) buildDB(database string) (driver.Conn, error) {
	dsnURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errConfigInvalidEndpoint, err.Error())
	}

	options := clickhouse.Options{
		Addr: []string{dsnURL.Host},
		Auth: clickhouse.Auth{
			Username: cfg.Username,
			Password: string(cfg.Password),
			Database: database,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{
					Name: "otel-clickhouse-exporter",
				},
			},
		},
		FreeBufOnConnRelease: true,
	}

	if _, ok := cfg.ConnectionParams["secure"]; ok {
		if _, ok := cfg.ConnectionParams["skip_verify"]; ok {
			options.TLS = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
	}

	return clickhouse.Open(&options)
}

// shouldCreateSchema returns true if the exporter should run the DDL for creating database/tables.
func (cfg *Config) shouldCreateSchema() bool {
	return cfg.CreateSchema
}

// tableEngineString generates the ENGINE string.
func (cfg *Config) tableEngineString() string {
	engine := cfg.TableEngine.Name
	params := cfg.TableEngine.Params

	if cfg.TableEngine.Name == "" {
		engine = defaultTableEngineName
		params = ""
	}

	return fmt.Sprintf("%s(%s)", engine, params)
}

// clusterString generates the ON CLUSTER string. Returns empty string if not set.
func (cfg *Config) clusterString() string {
	if cfg.ClusterName == "" {
		return ""
	}

	return fmt.Sprintf("ON CLUSTER %s", cfg.ClusterName)
}
