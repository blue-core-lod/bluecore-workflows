"""
One-off migration: re-map every user holding an old FAB-era client role on the
bluecore_workflows client (airflow_viewer/airflow_user/airflow_op/airflow_admin/
airflow_public) to its new equivalent (Viewer/User/Op/Admin/Public), assigned as
whichever role type (realm or client) actually exists under that name.

Usage (inside a container that already has python-keycloak, e.g. airflow-apiserver):
    python3 migrate_keycloak_roles.py \
        --server-url http://keycloak:8080/keycloak/ \
        --realm bluecore \
        --username admin --password *** --user-realm master --admin-client-id admin-cli \
        --workflows-client-id bluecore_workflows \
        --dry-run

Standalone (no Airflow container needed, just network access to Keycloak's admin API):
    uvx --with python-keycloak python3 migrate_keycloak_roles.py --server-url ... [...]

Drop --dry-run to actually apply. Add --remove-old to also strip the old client
role from each user after assigning the new one (leave this off on the first
pass so you can verify before cleaning up).
"""

import argparse
import sys

from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakGetError

# Old FAB-era client role -> new KeycloakAuthManager role name.
ROLE_MAP = {
    "airflow_viewer": "Viewer",
    "airflow_user": "User",
    "airflow_op": "Op",
    "airflow_admin": "Admin",
    "airflow_public": "Public",
}


def get_client_uuid(kc_admin: KeycloakAdmin, client_id: str) -> str:
    matches = [c for c in kc_admin.get_clients() if c["clientId"] == client_id]
    if not matches:
        raise ValueError(
            f"Client with ID='{client_id}' not found in realm '{kc_admin.connection.realm_name}'"
        )
    return matches[0]["id"]


def resolve_new_role(kc_admin: KeycloakAdmin, client_uuid: str, role_name: str):
    """Return (role_representation, kind) where kind is 'realm' or 'client'."""
    try:
        return kc_admin.get_realm_role(role_name), "realm"
    except KeycloakGetError:
        return kc_admin.get_client_role(client_uuid, role_name), "client"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server-url", required=True, help="e.g. http://keycloak:8080/keycloak/"
    )
    parser.add_argument("--realm", required=True, help="e.g. bluecore")
    parser.add_argument(
        "--username", required=True, help="admin username used to run the migration"
    )
    parser.add_argument("--password", required=True)
    parser.add_argument(
        "--user-realm", default="master", help="realm the admin user itself lives in"
    )
    parser.add_argument("--admin-client-id", default="admin-cli")
    parser.add_argument("--workflows-client-id", default="bluecore_workflows")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--remove-old",
        action="store_true",
        help="also strip the old client role from each user after assigning the new role",
    )
    args = parser.parse_args()

    kc_admin = KeycloakAdmin(
        server_url=args.server_url,
        username=args.username,
        password=args.password,
        realm_name=args.realm,
        user_realm_name=args.user_realm,
        client_id=args.admin_client_id,
        verify=True,
    )

    client_uuid = get_client_uuid(kc_admin, args.workflows_client_id)

    totals = {}
    for old_role, new_role in ROLE_MAP.items():
        try:
            members = kc_admin.get_client_role_members(client_uuid, old_role)
        except KeycloakGetError as exc:
            print(
                f"!! could not read members of old role '{old_role}': {exc}",
                file=sys.stderr,
            )
            continue

        try:
            new_role_rep, kind = resolve_new_role(kc_admin, client_uuid, new_role)
        except KeycloakGetError:
            print(
                f"!! new role '{new_role}' does not exist (realm or client) -- skipping '{old_role}'",
                file=sys.stderr,
            )
            continue

        try:
            old_role_rep = kc_admin.get_client_role(client_uuid, old_role)
        except KeycloakGetError:
            old_role_rep = None

        print(f"\n{old_role} -> {new_role} ({kind} role), {len(members)} user(s)")
        migrated = 0
        for user in members:
            username = user["username"]
            user_id = user["id"]
            if args.dry_run:
                print(f"  would assign '{new_role}' to {username}")
                if args.remove_old:
                    print(f"  would remove '{old_role}' from {username}")
                migrated += 1
                continue

            if kind == "realm":
                kc_admin.assign_realm_roles(user_id, new_role_rep)
            else:
                kc_admin.assign_client_role(user_id, client_uuid, new_role_rep)
            print(f"  assigned '{new_role}' to {username}")

            if args.remove_old and old_role_rep:
                kc_admin.delete_client_roles_of_user(user_id, client_uuid, old_role_rep)
                print(f"  removed '{old_role}' from {username}")

            migrated += 1

        totals[new_role] = migrated

    print("\nSummary:")
    for role, count in totals.items():
        print(
            f"  {role}: {count} user(s) {'would be ' if args.dry_run else ''}migrated"
        )


if __name__ == "__main__":
    main()
