"""
One-time setup script: creates the Databricks secret scope and stores the
Massive API key. Run this locally (with the Databricks CLI configured) or
from a notebook - never commit the resulting secret value anywhere.

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

w = WorkspaceClient()

# Create the secret scope if it doesn't exist
try:
    w.secrets.create_scope(scope="database_food_project")
    print("Created secret scope 'database_food_project'")
except Exception as e:
    print(f"Scope may already exist or creation failed: {e}")

w.secrets.put_secret(
    scope="database_food_project",
    key="lakebase-url",
    string_value=getpass.getpass("Paste your Lakebase URL: ")
)


w.secrets.put_acl(
    scope="database_food_project",
    principal="users",
    permission=workspace.AclPermission.READ,
)

