package config

import (
	"strings"
	"testing"
	"time"
)

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaults(t *testing.T) {
	c, err := LoadFrom(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.ReferenceTTL != 5*time.Minute || c.DBMaxConns != 20 {
		t.Fatalf("%+v", c)
	}
	if c.KeycloakIssuer != "http://localhost:8081/realms/wager" ||
		c.KeycloakJWKSURL != "http://localhost:8081/realms/wager/protocol/openid-connect/certs" ||
		c.KeycloakAudience != "wager-api" {
		t.Fatalf("keycloak defaults: %+v", c)
	}
}

func TestIssuerCanDifferFromJWKSHost(t *testing.T) {
	c, err := LoadFrom(lookup(map[string]string{
		"KEYCLOAK_URL": "http://keycloak:8080/", "KEYCLOAK_ISSUER": "http://localhost:8081/realms/wager",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.KeycloakIssuer != "http://localhost:8081/realms/wager" ||
		c.KeycloakJWKSURL != "http://keycloak:8080/realms/wager/protocol/openid-connect/certs" {
		t.Fatalf("%+v", c)
	}
}

func TestInvalidValuesAreReported(t *testing.T) {
	_, err := LoadFrom(lookup(map[string]string{
		"REFERENCE_TTL": "abc", "DB_MAX_CONNS": "0", "LOG_LEVEL": "loud", "PENDING_POLL_INTERVAL": "-1s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"REFERENCE_TTL", "DB_MAX_CONNS", "LOG_LEVEL", "PENDING_POLL_INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}
