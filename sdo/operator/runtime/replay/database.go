package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type databaseCatalog struct {
	target program.DatabaseTarget
	users  []program.DatabaseUser
}

func databaseKey(t program.DatabaseTarget) string {
	t.Auth = nil
	t.Shell = ""
	raw, _ := json.Marshal(t)
	return string(raw)
}
func (c *cluster) seedDatabases(calls []sdktest.PrimitiveCall) error {
	c.catalogs = map[string]databaseCatalog{}
	c.catalogConfigurations = map[string]program.DatabaseTarget{}
	for _, call := range calls {
		if call.Method != "DatabaseUsers" || call.Error != "" {
			continue
		}
		var targets []program.DatabaseTarget
		if err := json.Unmarshal(call.Arguments, &targets); err != nil || len(targets) != 1 {
			return fmt.Errorf("invalid database catalog recording")
		}
		target := targets[0]
		if err := target.Validate(); err != nil {
			return err
		}
		var users []program.DatabaseUser
		if err := json.Unmarshal(call.Result, &users); err != nil {
			return fmt.Errorf("invalid database catalog recording")
		}
		normalized, err := program.NormalizeDatabaseUsers(users)
		if err != nil {
			return err
		}
		key := databaseKey(target)
		if old, ok := c.catalogs[key]; ok {
			a, _ := json.Marshal(old.users)
			b, _ := json.Marshal(normalized)
			if string(a) != string(b) {
				return fmt.Errorf("conflicting open database catalogs")
			}
		}
		c.catalogs[key] = databaseCatalog{target: target, users: normalized}
		c.catalogConfigurations[databaseConfigurationKey(target)] = target
	}
	return nil
}
func (c *cluster) DatabaseUsers(ctx context.Context, t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if _, ok := c.catalogConfigurations[databaseConfigurationKey(t)]; !ok {
		return nil, fmt.Errorf("database target auth/shell configuration was not recorded at open")
	}
	catalog, ok := c.catalogs[databaseKey(t)]
	if !ok {
		return nil, fmt.Errorf("database catalog was not recorded at open")
	}
	if _, err := c.Get(ctx, t.Pod); err != nil {
		return nil, err
	}
	return program.NormalizeDatabaseUsers(catalog.users)
}
func (c *cluster) EnsureDatabaseUser(ctx context.Context, a program.EnsureDatabaseUser) error {
	users, err := c.DatabaseUsers(ctx, a.Target)
	if err != nil {
		return err
	}
	next, err := program.ApplyEnsureDatabaseUser(users, a)
	if err != nil {
		return err
	}
	c.catalogs[databaseKey(a.Target)] = databaseCatalog{target: a.Target, users: next}
	return nil
}
func (v *readView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if v.catalog != nil {
		return v.catalog.DatabaseUsers(v.ctx, t)
	}
	return v.ReplayReadView.DatabaseUsers(t)
}

func databaseConfigurationKey(t program.DatabaseTarget) string {
	if t.Shell == "" {
		t.Shell = "mongo"
	}
	raw, _ := json.Marshal(t)
	return string(raw)
}
