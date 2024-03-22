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
	// TracesTableName is the table name for logs. default is `otel_traces`.
	TracesTableName string `mapstructure:"traces_table_name"`
	// MetricsTableName is the table name for metrics. default is `otel_metrics`.
	MetricsTableName string `mapstructure:"metrics_table_name"`
	// TTLDays is The data time-to-live in days, 0 means no ttl.
	// Deprecated: Use 'ttl' instead
	TTLDays uint `mapstructure:"ttl_days"`
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`

	// Sharded is the configuration to allow the DDL creation of tables
	Sharded bool `mapstructure:"sharded"`
}

const defaultDatabase = "default"

var (
	errConfigNoEndpoint      = errors.New("endpoint must be specified")
	errConfigInvalidEndpoint = errors.New("endpoint must be url format")
	errConfigTTL             = errors.New("both 'ttl_days' and 'ttl' can not be provided. 'ttl_days' is deprecated, use 'ttl' instead")
)

// Validate the clickhouse server configuration.
func (cfg *Config) Validate() (err error) {
	if cfg.Endpoint == "" {
		err = errors.Join(err, errConfigNoEndpoint)
	}
	dsn, e := cfg.buildDSN(cfg.Database)
	if e != nil {
		err = errors.Join(err, e)
	}

	if cfg.TTL > 0 && cfg.TTLDays > 0 {
		err = errors.Join(err, errConfigTTL)
	}

	// Validate DSN with clickhouse driver.
	// Last chance to catch invalid config.
	if _, e := clickhouse.ParseDSN(dsn); e != nil {
		err = errors.Join(err, e)
	}

	return err
}

func (cfg *Config) buildDSN(database string) (string, error) {
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

	// Override database if specified in config.
	if cfg.Database != "" {
		dsnURL.Path = cfg.Database
	}

	// Override database if specified in database param.
	if database != "" {
		dsnURL.Path = database
	}

	// Use default database if not specified in any other place.
	if database == "" && cfg.Database == "" && dsnURL.Path == "" {
		dsnURL.Path = defaultDatabase
	}

	// Override username and password if specified in config.
	if cfg.Username != "" {
		dsnURL.User = url.UserPassword(cfg.Username, string(cfg.Password))
	}

	dsnURL.RawQuery = queryParams.Encode()

	return dsnURL.String(), nil
}

func (cfg *Config) buildDB(database string) (driver.Conn, error) {
	options := clickhouse.Options{
		Addr: []string{cfg.Endpoint},
		Auth: clickhouse.Auth{
			Username: cfg.Username,
			Password: string(cfg.Password),
			Database: cfg.Database,
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
