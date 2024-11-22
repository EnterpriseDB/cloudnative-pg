/*
Copyright The CloudNativePG Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package roles

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"

	"github.com/jackc/pgx/v5/pgtype"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
)

// DatabaseRole represents the role information read from / written to the Database
// The password management in the apiv1.RoleConfiguration assumes the use of Secrets,
// so cannot cleanly be mapped to Postgres
type DatabaseRole struct {
	Name            string           `json:"name"`
	Comment         string           `json:"comment,omitempty"`
	Superuser       bool             `json:"superuser,omitempty"`
	CreateDB        bool             `json:"createdb,omitempty"`
	CreateRole      bool             `json:"createrole,omitempty"`
	Inherit         bool             `json:"inherit,omitempty"` // defaults to true
	Login           bool             `json:"login,omitempty"`
	Replication     bool             `json:"replication,omitempty"`
	BypassRLS       bool             `json:"bypassrls,omitempty"` // Row-Level Security
	IgnorePassword  bool             `json:"-"`
	ConnectionLimit int64            `json:"connectionLimit,omitempty"` // default is -1
	ValidUntil      pgtype.Timestamp `json:"validUntil,omitempty"`
	InRoles         []string         `json:"inRoles,omitempty"`
	Password        sql.NullString   `json:"-"`
	TransactionID   int64            `json:"-"`
}

// passwordNeedsUpdating evaluates whether a DatabaseRole needs to be updated
func (d *DatabaseRole) passwordNeedsUpdating(
	storedPasswordState map[string]apiv1.PasswordState,
	latestSecretResourceVersion map[string]string,
) bool {
	return storedPasswordState[d.Name].SecretResourceVersion != latestSecretResourceVersion[d.Name] ||
		storedPasswordState[d.Name].TransactionID != d.TransactionID
}

func (d *DatabaseRole) hasSameCommentAs(inSpec apiv1.RoleConfiguration) bool {
	return d.Comment == inSpec.Comment
}

func (d *DatabaseRole) isInSameRolesAs(inSpec apiv1.RoleConfiguration) bool {
	if len(d.InRoles) == 0 && len(inSpec.InRoles) == 0 {
		return true
	}

	if len(d.InRoles) != len(inSpec.InRoles) {
		return false
	}

	sort.Strings(d.InRoles)
	sort.Strings(inSpec.InRoles)
	return reflect.DeepEqual(d.InRoles, inSpec.InRoles)
}

func (d *DatabaseRole) hasSameValidUntilAs(inSpec apiv1.RoleConfiguration) bool {
	if inSpec.ValidUntil == nil {
		return !d.ValidUntil.Valid || d.ValidUntil.InfinityModifier == pgtype.Infinity
	}
	return d.ValidUntil.Valid && d.ValidUntil.Time.Equal(inSpec.ValidUntil.Time)
}

// isEquivalentTo checks a subset of the attributes of roles in DB and Spec
// leaving passwords and role membership (InRoles) to be done separately
func (d *DatabaseRole) isEquivalentTo(inSpec apiv1.RoleConfiguration) bool {
	type reducedEntries struct {
		Name            string
		Superuser       bool
		CreateDB        bool
		CreateRole      bool
		Inherit         bool
		Login           bool
		Replication     bool
		BypassRLS       bool
		ConnectionLimit int64
	}
	role := reducedEntries{
		Name:            d.Name,
		Superuser:       d.Superuser,
		CreateDB:        d.CreateDB,
		CreateRole:      d.CreateRole,
		Inherit:         d.Inherit,
		Login:           d.Login,
		Replication:     d.Replication,
		BypassRLS:       d.BypassRLS,
		ConnectionLimit: d.ConnectionLimit,
	}
	spec := reducedEntries{
		Name:            inSpec.Name,
		Superuser:       inSpec.Superuser,
		CreateDB:        inSpec.CreateDB,
		CreateRole:      inSpec.CreateRole,
		Inherit:         inSpec.GetRoleInherit(),
		Login:           inSpec.Login,
		Replication:     inSpec.Replication,
		BypassRLS:       inSpec.BypassRLS,
		ConnectionLimit: inSpec.ConnectionLimit,
	}

	return reflect.DeepEqual(role, spec) && d.hasSameValidUntilAs(inSpec)
}

// ApplyPassword updates a database role with the password located in the Secret
// it returns the resource version of the Secret
func (d *DatabaseRole) ApplyPassword(
	ctx context.Context,
	cl client.Client,
	rolePassword passwordManager,
	namespace string,
) (string, error) {
	var passVersion string
	switch {
	case rolePassword.GetRoleSecretName() == "" && !rolePassword.ShouldDisablePassword():
		d.IgnorePassword = true
		return "", nil
	case rolePassword.GetRoleSecretName() == "" && rolePassword.ShouldDisablePassword():
		d.Password = sql.NullString{}
		return "", nil
	case rolePassword.GetRoleSecretName() != "" && rolePassword.ShouldDisablePassword():
		// this case should be prevented by the validation webhook,
		// and is an error
		return "",
			fmt.Errorf("cannot reconcile: password both provided and disabled: %s",
				rolePassword.GetRoleSecretName())
	default: // role.PasswordSecret != nil && !rolePassword.ShouldDisablePassword():
		passwordSecret, err := getPassword(ctx, cl, rolePassword, namespace)
		if err != nil {
			return "", err
		}

		d.Password = sql.NullString{Valid: true, String: passwordSecret.password}
		passVersion = passwordSecret.version
		return passVersion, nil
	}
}
