"""
Pipedream Python Workflow - Deploy Backup/Restore Container
"""

import base64
import boto3
import time
import os
from datetime import datetime
from botocore.exceptions import ClientError


def load_config():
    """Load all configuration from environment variables"""
    s3_bucket = os.environ["S3_BUCKET"]
    s3_bucket_prefix = os.environ.get("S3_BUCKET_PREFIX", "")
    s3_bucket_url = (
        f"s3://{s3_bucket}/{s3_bucket_prefix}" if s3_bucket_prefix else f"s3://{s3_bucket}"
    )

    backblaze_key_id = os.environ.get("B2_APPLICATION_KEY_ID", "")
    backblaze_app_key = os.environ.get("B2_APPLICATION_KEY", "")
    backblaze_bucket = os.environ.get("B2_BUCKET", "")
    backblaze_endpoint = os.environ.get("B2_HOST", "")
    sentry_dsn = os.environ.get("SENTRY_DSN", "")

    def clean(s):
        """Remove all whitespace — for identifiers that must never contain newlines."""
        return "".join(s.split())

    def sanitize_password(s):
        """Strip outer whitespace and remove accidental newlines from copy-paste."""
        return s.strip().replace("\r", "").replace("\n", "")

    db_user = clean(os.environ["DB_USER"])
    db_userpassword = sanitize_password(os.environ["DB_USERPASSWORD"])
    db_rootuser = clean(os.environ.get("DB_ROOTUSER") or db_user)
    db_rootpassword = sanitize_password(
        os.environ.get("DB_ROOTPASSWORD") or db_userpassword
    )
    # Maintenance DB *name* (not a username)
    pg_database = clean(os.environ.get("PGDATABASE", "postgres"))

    mode = clean(os.environ.get("MODE", "backup")).lower()

    db_options = os.environ.get(
        "DB_OPTIONS",
        "--verbose" if mode != "restore" else "",
    ).strip()
    if mode == "restore":
        tokens = [t for t in db_options.split() if t not in ("--verbose", "-v")]
        db_options = " ".join(tokens)

    environment = {
        "MODE": mode,
        "DB_HOST": clean(os.environ["DB_HOST"]),
        "DB_PORT": clean(os.environ.get("DB_PORT", "5432")),
        "DB_NAME": clean(os.environ["DB_NAME"]),
        "DB_USER": db_user,
        "DB_USERPASSWORD": db_userpassword,
        "PGPASSWORD": db_rootpassword if mode == "restore" else db_userpassword,
        "DB_ROOTUSER": db_rootuser,
        "DB_ROOTPASSWORD": db_rootpassword,
        "PGDATABASE": pg_database,
        "DB_OPTIONS": db_options,
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"].strip(),
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"].strip(),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1").strip(),
        "S3_BUCKET": s3_bucket_url,
        "APP_ENV": os.environ.get("APP_ENV", "prod").strip(),
    }

    if backblaze_key_id and backblaze_app_key and backblaze_bucket:
        environment.update({
            "B2_APPLICATION_KEY_ID": backblaze_key_id,
            "B2_APPLICATION_KEY": backblaze_app_key,
            "B2_BUCKET": backblaze_bucket,
            "B2_HOST": backblaze_endpoint,
        })

    if sentry_dsn:
        environment["SENTRY_DSN"] = sentry_dsn

    return {
        "region": os.environ.get("AWS_REGION", "us-east-1").strip(),
        "service_name": os.environ["LIGHTSAIL_SERVICE_NAME"].strip(),
        "container_name": os.environ["LIGHTSAIL_CONTAINER_NAME"].strip(),
        "docker_image": os.environ["DOCKER_IMAGE"].strip(),
        "container_power": os.environ.get("CONTAINER_POWER", "medium"),
        "container_scale": int(os.environ.get("CONTAINER_SCALE", "1")),
        "polling_interval_seconds": int(os.environ.get("POLLING_INTERVAL_SECONDS", "60")),
        "max_polling_attempts": int(os.environ.get("MAX_POLLING_ATTEMPTS", "40")),
        "max_polling_duration_minutes": int(
            os.environ.get("MAX_POLLING_DURATION_MINUTES", "45")
        ),
        "initial_delay_seconds": int(os.environ.get("INITIAL_DELAY_SECONDS", "300")),
        "db_name": os.environ["DB_NAME"],
        "db_host": os.environ["DB_HOST"],
        "s3_bucket_url": s3_bucket_url,
        "backblaze_bucket": backblaze_bucket,
        "sentry_dsn": sentry_dsn,
        "environment": environment,
        "mode": mode,
    }


def container_service_exists(lightsail, service_name):
    """Check if container service exists"""
    try:
        response = lightsail.get_container_services(serviceName=service_name)
        return len(response["containerServices"]) > 0
    except ClientError as e:
        if e.response["Error"]["Code"] == "NotFoundException":
            return False
        raise


def create_container_service(lightsail, service_name, power, scale):
    """Create Lightsail container service"""
    print(f"Creating container service '{service_name}'...")
    print(f"  Power: {power}")
    print(f"  Scale: {scale}")
    lightsail.create_container_service(
        serviceName=service_name, power=power, scale=scale
    )
    print("  Waiting for container service to become active...")
    max_wait, waited = 300, 0
    while waited < max_wait:
        service = lightsail.get_container_services(serviceName=service_name)[
            "containerServices"
        ][0]
        state = service.get("state")
        if state in ["ACTIVE", "READY"]:
            print("  Container service is active and ready")
            return True
        if state in ["PENDING", "DEPLOYING"]:
            print(f"  Status: {state}... waiting")
            time.sleep(10)
            waited += 10
        else:
            print(f"  Unexpected state: {state}")
            return False
    print(f"  Warning: Service creation took longer than {max_wait} seconds")
    return False


def enable_container_service(lightsail, service_name):
    """Enable container service if disabled"""
    print("Checking container service status...")
    service = lightsail.get_container_services(serviceName=service_name)[
        "containerServices"
    ][0]
    if service.get("isDisabled", False):
        print("  Container service is disabled, enabling it...")
        lightsail.update_container_service(serviceName=service_name, isDisabled=False)
        print("  Container service enabled")
        time.sleep(5)
    else:
        print("  Container service is already enabled")


def _restore_script_patcher():
    """
    PG 17 same-version: only fix DROP path.
    Do not strip PG17 dump features (transaction_timeout, LOCALE_PROVIDER, etc.).
    """
    return r"""
from pathlib import Path

path = Path("/data/restore.sh")
text = path.read_text()

text = text.replace(
    '--username="${db_owner}"',
    '--username="${DB_ROOTUSER}"',
)
text = text.replace(
    "DROP DATABASE ${DB_NAME} WITH (FORCE);",
    "DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE);",
)
text = text.replace(
    "DROP DATABASE ${DB_NAME};",
    "DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE);",
)

path.write_text(text)
print("Patched restore.sh: DROP uses DB_ROOTUSER + IF EXISTS WITH (FORCE) [PG17]")
"""


def build_container_command(mode):
    """
    One-shot jobs: idle after success so Lightsail does not restart/re-run.
    Restore also patches DROP to use DB_ROOTUSER + IF EXISTS FORCE.
    """
    idle_after_success = (
        "./entrypoint.sh; status=$?; "
        "if [ \"$status\" -eq 0 ]; then "
        f"echo '{mode.capitalize()} finished - idling to prevent Lightsail restart loop'; "
        "sleep infinity; "
        "fi; "
        "exit $status"
    )

    if mode != "restore":
        return ["bash", "-lc", idle_after_success]

    encoded = base64.b64encode(_restore_script_patcher().encode("utf-8")).decode("ascii")
    return [
        "bash",
        "-lc",
        f"echo '{encoded}' | base64 -d | python3 && {idle_after_success}",
    ]


def deploy_container(lightsail, config):
    """Deploy backup or restore container"""
    mode = config.get("mode", "backup")
    print(f"\nDeploying {mode} container...")
    command = build_container_command(mode)
    if mode == "restore":
        print("Restore: DROP as DB_ROOTUSER + IF EXISTS FORCE; idle after success")
    else:
        print("Backup: idle after success to prevent Lightsail restart loop")

    deployment = {
        "serviceName": config["service_name"],
        "containers": {
            config["container_name"]: {
                "image": config["docker_image"],
                "command": command,
                "environment": config["environment"],
                "ports": {},
            }
        },
    }
    response = lightsail.create_container_service_deployment(**deployment)
    cs = response["containerService"]
    if "currentDeployment" in cs:
        version = cs["currentDeployment"]["version"]
    elif "nextDeployment" in cs:
        version = cs["nextDeployment"]["version"]
    else:
        version = "unknown"
    print(f"  Container deployed (version {version})")
    return version


def handler(pd):  # noqa: ARG001 pylint: disable=W0613
    """Deploy backup/restore container and return initial monitor state"""
    config = load_config()
    mode = config["mode"]
    label = "RESTORE" if mode == "restore" else "BACKUP"

    print("=" * 60)
    print(f"{label} DEPLOYMENT WORKFLOW (Pipedream + Lightsail)")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Database: {config['db_name']} on {config['db_host']}")
    print(f"S3: {config['s3_bucket_url']}")
    print(f"DB_USER (from env): {config['environment']['DB_USER']}")
    print(f"DB_ROOTUSER (from env): {config['environment']['DB_ROOTUSER']}")
    print(f"DB_OPTIONS: {config['environment'].get('DB_OPTIONS') or '(none)'}")
    if config["backblaze_bucket"]:
        print(f"Backblaze: {config['backblaze_bucket']}")
    if config["sentry_dsn"]:
        print("Sentry monitoring: Enabled")
    print()

    lightsail = boto3.client("lightsail", region_name=config["region"])

    try:
        if not container_service_exists(lightsail, config["service_name"]):
            print(f"Container service '{config['service_name']}' does not exist")
            create_container_service(
                lightsail,
                config["service_name"],
                config["container_power"],
                config["container_scale"],
            )
        else:
            print(f"Container service '{config['service_name']}' already exists")
            enable_container_service(lightsail, config["service_name"])

        deployment_version = deploy_container(lightsail, config)
        now = datetime.now()
        initial_state = {
            "polling_attempt": 1,
            "first_started_at": now.isoformat(),
            "last_checked_at": now.isoformat(),
            "polling_interval_seconds": config["polling_interval_seconds"],
            "max_polling_attempts": config["max_polling_attempts"],
            "max_polling_duration_minutes": config["max_polling_duration_minutes"],
            "deployment_version": str(deployment_version),
            "deployment_time": now.isoformat(),
            "mode": mode,
        }

        print("\n" + "=" * 60)
        print("Deployment Completed")
        print("=" * 60)
        print(f"Service: {config['service_name']}")
        print(f"Container: {config['container_name']}")
        print(f"Version: {deployment_version}")
        print(f"Time: {now.isoformat()}")
        print(f"Initial delay: {config['initial_delay_seconds']} seconds")
        print("=" * 60)

        return {
            "status": "deployed",
            "service_name": config["service_name"],
            "container_name": config["container_name"],
            "deployment_version": deployment_version,
            "deployment_time": now.isoformat(),
            "initial_state": initial_state,
            "message": "Container deployed. Monitor will start after delay.",
        }
    except Exception as error:
        print(f"\nDeployment Failed: {error}")
        raise
