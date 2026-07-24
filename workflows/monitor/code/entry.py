"""
Pipedream Python Workflow - Monitor Backup and Cleanup
"""

import boto3
import os
import requests
from datetime import datetime, timedelta, timezone

def load_config():
    """Load all configuration from environment variables"""
    s3_bucket = os.environ["S3_BUCKET"]
    s3_bucket_prefix = os.environ.get("S3_BUCKET_PREFIX", "")

    return {
        # Lightsail service config
        "region": os.environ.get("AWS_REGION", "us-east-1").strip(),
        "service_name": os.environ["LIGHTSAIL_SERVICE_NAME"].strip(),
        "container_name": os.environ["LIGHTSAIL_CONTAINER_NAME"].strip(),

        # S3 config
        "s3_bucket": s3_bucket,
        "s3_bucket_prefix": s3_bucket_prefix,

        # Backblaze config (using B2_* names to match container expectations)
        "backblaze_key_id": os.environ.get("B2_APPLICATION_KEY_ID", ""),
        "backblaze_app_key": os.environ.get("B2_APPLICATION_KEY", ""),
        "backblaze_bucket": os.environ.get("B2_BUCKET", ""),
        "backblaze_endpoint": os.environ.get("B2_HOST", ""),

        # Cleanup behavior
        "destroy_on_completion": os.environ.get("DESTROY_ON_COMPLETION", "true").lower() == "true",
        "mode": os.environ.get("MODE", "backup").strip().lower(),

        # Polling config
        "polling_interval_seconds": int(os.environ.get("POLLING_INTERVAL_SECONDS", "60")),
        "max_polling_attempts": int(os.environ.get("MAX_POLLING_ATTEMPTS", "40")),
        "max_polling_duration_minutes": int(os.environ.get("MAX_POLLING_DURATION_MINUTES", "45")),
        "initial_delay_seconds": int(os.environ.get("INITIAL_DELAY_SECONDS", "300")),
        "monitor_webhook_url": os.environ.get("MONITOR_WEBHOOK_URL", ""),
    }

def _debug_trigger_data(pd):
    print("DEBUG: Trigger Data")

    # pd.steps is a dict, use dict notation
    try:
        if 'trigger' in pd.steps:
            trigger = pd.steps['trigger']
            print("trigger exists: True")
            print(f"trigger keys: {trigger.keys() if hasattr(trigger, 'keys') else 'N/A'}")
            if 'event' in trigger:
                print(f"trigger['event']: {trigger['event']}")
            if 'body' in trigger:
                print(f"trigger['body']: {trigger['body']}")
        else:
            print("trigger not found in pd.steps")
    except Exception as e:
        print(f"trigger access failed: {e}")

    print("=== END DEBUG ===")

def _get_state_from_event(pd):
    """Get state from trigger.event"""
    try:
        if 'trigger' in pd.steps:
            trigger = pd.steps['trigger']
            if 'event' in trigger:
                event = trigger['event']

                if isinstance(event, dict) and 'polling_attempt' in event:
                    invocation_num = event.get('polling_attempt', 1)
                    if invocation_num == 1:
                        print("Received initial state from HTTP webhook via trigger['event']")
                    else:
                        print(f"Restoring state from HTTP self-trigger - invocation #{invocation_num}")
                    return event

    except (AttributeError, KeyError, TypeError):
        pass

    return None

def _get_state_from_body(pd):
    if not (hasattr(pd.steps, 'trigger') and hasattr(pd.steps.trigger, 'body')):
        return None
    
    body = pd.steps.trigger.body
    print(f"HTTP body received: {body}")

    if isinstance(body, dict) and 'polling_attempt' in body:
        print("Using HTTP body as initial state (direct)")
        return body

    if isinstance(body, dict) and 'initial_state' in body:
        print("Received initial state from HTTP webhook (nested)")
        return body['initial_state']
    
    return None

def _get_state_from_query(pd):
    if not (hasattr(pd.steps, 'trigger') and hasattr(pd.steps.trigger, 'query')):
        return None
    
    query = pd.steps.trigger.query
    print(f"HTTP query received: {query}")
    
    if 'data' in query:
        import json
        try:
            state = json.loads(query['data'])
            print("Received initial state from query parameter")
            return state
        except ValueError as e:
            print(f"Failed to parse query data: {query['data']} - {e}")
    
    return None

def get_job_mode(state, config):
    """Resolve job mode from polling state or environment."""
    mode = state.get("mode") or config.get("mode", "backup")
    return str(mode).strip().lower()

def get_job_label(mode):
    return "Restore" if mode == "restore" else "Backup"

def _create_default_state(config):
    now = datetime.now().isoformat()
    state = {
        "polling_attempt": 1,
        "first_started_at": now,
        "last_checked_at": now,
        "polling_interval_seconds": config["polling_interval_seconds"],
        "max_polling_attempts": config["max_polling_attempts"],
        "max_polling_duration_minutes": config["max_polling_duration_minutes"],
        "previous_container_state": None,
        "backup_found_in_s3": False,
        "restart_detected": False,
        "mode": config.get("mode", "backup"),
    }
    print("Created fallback initial state")
    return state

def initialize_state(pd, config):
    """Initialize or restore state"""
    _debug_trigger_data(pd)

    state = _get_state_from_event(pd)
    if state:
        return state

    state = _get_state_from_body(pd)
    if state:
        return state

    state = _get_state_from_query(pd)
    if state:
        return state

    return _create_default_state(config)

def calculate_duration(start_time_iso):
    """Calculate duration in minutes"""
    start_time = datetime.fromisoformat(start_time_iso.replace('Z', '+00:00') if start_time_iso.endswith('Z') else start_time_iso)
    now = datetime.now()
    if start_time.tzinfo is not None:
        start_time = start_time.replace(tzinfo=None)
    duration_seconds = (now - start_time).total_seconds()
    return duration_seconds / 60

def check_timeout_conditions(state):
    """Check if polling should stop due to timeout"""
    attempt = state["polling_attempt"]
    max_attempts = state["max_polling_attempts"]

    if attempt > max_attempts:
        duration = calculate_duration(state["first_started_at"])
        return {
            'status': 'timeout_max_attempts',
            'polling_attempts': attempt,
            'max_attempts': max_attempts,
            'duration_minutes': round(duration, 1),
            'message': f'Polling timeout: Exceeded {max_attempts} attempts ({duration:.1f} minutes)',
        }

    duration = calculate_duration(state["first_started_at"])
    max_duration = state["max_polling_duration_minutes"]

    if duration > max_duration:
        return {
            'status': 'timeout_max_duration',
            'polling_attempts': attempt,
            'duration_minutes': round(duration, 1),
            'max_duration': max_duration,
            'message': f'Polling timeout: Exceeded {max_duration} minutes',
        }

    return None

def log_polling_status(state, mode):
    """Log current polling status"""
    attempt = state["polling_attempt"]
    max_attempts = state["max_polling_attempts"]
    duration = calculate_duration(state["first_started_at"])
    job_label = get_job_label(mode)

    print("=" * 60)
    print(f"{job_label} Monitor - Polling Attempt {attempt}/{max_attempts}")
    print("=" * 60)
    print(f"Started at: {state['first_started_at']}")
    print(f"Duration: {duration:.1f} minutes")
    print(f"Interval: {state['polling_interval_seconds']} seconds")
    print()

def schedule_next_poll_and_return(state, config, mode):
    """Schedule next polling attempt"""
    next_state = {
        **state,
        "polling_attempt": state["polling_attempt"] + 1,
        "last_checked_at": datetime.now().isoformat(),
        "mode": mode,
    }

    polling_interval = state["polling_interval_seconds"]
    next_attempt = next_state["polling_attempt"]
    max_attempts = state["max_polling_attempts"]
    duration = calculate_duration(state["first_started_at"])

    job_label = get_job_label(mode)

    print("\n" + "=" * 60)
    print(f"{job_label} Still In Progress")
    print("=" * 60)
    print(f"Current attempt: {state['polling_attempt']}/{max_attempts}")
    print(f"Total duration: {duration:.1f} minutes")
    print(f"Next check in: {polling_interval} seconds")
    print(f"Next attempt: {next_attempt}/{max_attempts}")
    print("=" * 60)

    monitor_url = config.get("monitor_webhook_url", "")

    if not monitor_url:
        print("\nWARNING: MONITOR_WEBHOOK_URL not set - cannot schedule next poll")
        print("Set MONITOR_WEBHOOK_URL environment variable to enable polling")
        return {
            'status': 'error',
            'message': 'MONITOR_WEBHOOK_URL not configured - polling disabled',
            'polling_attempt': state['polling_attempt'],
        }

    try:
        print(f"\nTriggering next poll via HTTP request to: {monitor_url[:50]}...")

        response = requests.post(
            monitor_url,
            json=next_state,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        print(f"HTTP trigger response: {response.status_code}")
        if response.status_code == 200:
            print("Next poll triggered successfully")
            print(f"Note: Add a 'Delay' step ({polling_interval}s) in Pipedream before this code step")
        else:
            print(f"WARNING: Unexpected response code: {response.status_code}")

    except Exception as e:
        print(f"\nERROR: Failed to trigger next poll: {e}")
        print(f"Exception type: {type(e).__name__}")
        print("Manual intervention required - polling chain broken!")

    return {
        'status': 'in_progress',
        'polling_attempt': state['polling_attempt'],
        'max_attempts': max_attempts,
        'duration_minutes': round(duration, 1),
        'next_check_seconds': polling_interval,
        'message': f'{job_label} in progress. Next check scheduled.',
    }

def is_job_complete(log_events, mode):
    """Check if backup or restore is complete based on logs"""
    if not log_events:
        return False

    # Prefer the latest run only — older failures/successes must not confuse us.
    log_text = ' '.join(_last_job_slice(log_events, mode)).lower()

    if mode == "restore":
        # Image logs: "restore: Completed" (not "...successfully")
        completion_keywords = [
            'restore: completed',
            'database successfully restored and verified',
            'temporary files cleaned up',
            'idling to prevent lightsail restart loop',
        ]
    else:
        # Image logs: "backup: Completed" (not "...successfully")
        completion_keywords = [
            'backup: completed',
            'cleaning up temporary files',
            'exiting with status 0',
            'backup process finished',
            'idling to prevent lightsail restart loop',
        ]

    return any(keyword in log_text for keyword in completion_keywords)

def detect_job_failure(log_events, mode="backup"):
    """Detect fatal errors in the most recent job run."""
    if not log_events:
        return False, None

    messages = _last_job_slice(log_events, mode)
    log_text = ' '.join(messages).lower()

    # Success in the latest run wins over older failure text elsewhere in logs.
    if mode == "restore" and 'restore: completed' in log_text:
        return False, None
    if mode != "restore" and 'backup: completed' in log_text:
        return False, None

    failure_patterns = [
        'password authentication failed',
        'non-zero exit:',
        'error: fatal:',
    ]

    for pattern in failure_patterns:
        if pattern in log_text:
            return True, pattern

    return False, None

def detect_multiple_job_runs(log_events, mode):
    """Detect if backup or restore has run multiple times"""
    if not log_events:
        return False, 0

    start_marker = 'restore: Started' if mode == "restore" else 'backup: Started'
    job_starts = sum(1 for message in _log_messages(log_events) if start_marker in message)

    return job_starts > 1, job_starts

def handle_error(error, state):
    """Handle errors during polling"""
    duration = calculate_duration(state["first_started_at"])

    print(f"\nMonitor Workflow Error: {error}")
    print(f"Polling attempt: {state['polling_attempt']}")
    print(f"Duration: {duration:.1f} minutes")

    return {
        'status': 'error',
        'error': str(error),
        'polling_attempt': state['polling_attempt'],
        'duration_minutes': round(duration, 1),
        'message': 'Error occurred during polling. Check logs for details.',
    }

def get_container_status(lightsail, config):
    """Get current container status"""
    from botocore.exceptions import ClientError

    try:
        response = lightsail.get_container_services(serviceName=config['service_name'])
        service = response['containerServices'][0]

        return {
            'state': service['state'],
            'is_disabled': service.get('isDisabled', False),
            'current_deployment': service.get('currentDeployment', {}),
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'NotFoundException':
            return {
                'state': 'NOT_FOUND',
                'is_disabled': False,
                'current_deployment': {},
            }
        raise

def get_recent_logs(lightsail, config, max_lines=200, filter_pattern=None, lookback_minutes=90):
    """
    Fetch container logs from Lightsail.

    Without a filter, Lightsail often returns only the first page of noisy SQL
    output (CREATE/COPY), which hides "backup: Completed" / "restore: Completed".
    Prefer filter_pattern for status detection.
    """
    try:
        start_time = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        log_events = []
        page_token = None
        pages = 0

        while pages < 20:
            kwargs = {
                "serviceName": config["service_name"],
                "containerName": config["container_name"],
                "startTime": start_time,
            }
            if filter_pattern is not None:
                kwargs["filterPattern"] = filter_pattern
            if page_token:
                kwargs["pageToken"] = page_token

            response = lightsail.get_container_log(**kwargs)
            log_events.extend(response.get("logEvents", []))
            page_token = response.get("nextPageToken")
            pages += 1
            if not page_token:
                break

        return log_events[-max_lines:] if log_events else []
    except Exception as e:
        print(f"  Warning: Could not fetch logs: {e}")
        return []

def get_job_status_logs(lightsail, config, mode):
    """Fetch only status-relevant log lines (not SQL dump noise)."""
    # OR-match status markers. Avoid matching generic CREATE/COPY/ALTER lines.
    if mode == "restore":
        filter_pattern = "?restore: ?idling ?Patched ?FATAL ?Non-zero ?successfully restored"
    else:
        filter_pattern = "?backup: ?idling ?FATAL ?Non-zero"

    events = get_recent_logs(
        lightsail,
        config,
        max_lines=300,
        filter_pattern=filter_pattern,
    )
    if events:
        return events

    # Fallback: unfiltered (may be noisy / incomplete on Lightsail)
    print("  Status filter returned no lines - falling back to unfiltered logs")
    return get_recent_logs(lightsail, config, max_lines=200)
def _log_messages(log_events):
    return [e.get('message', '') for e in log_events]

def _last_job_slice(log_events, mode):
    """Return log messages from the most recent job start onward."""
    messages = _log_messages(log_events)
    start_marker = 'restore: Started' if mode == "restore" else 'backup: Started'
    last_start = -1
    for i, message in enumerate(messages):
        if start_marker in message:
            last_start = i
    if last_start < 0:
        return messages
    return messages[last_start:]

def is_backup_complete(log_events):
    """Backward-compatible wrapper for backup completion checks."""
    return is_job_complete(log_events, "backup")

def detect_multiple_backup_runs(log_events):
    """Backward-compatible wrapper for backup restart detection."""
    multiple_runs, job_starts = detect_multiple_job_runs(log_events, "backup")
    return multiple_runs, job_starts

def verify_backup_in_s3(s3, config):
    """Verify backup files in S3"""
    print("\nVerifying backup in S3")

    prefix = config['s3_bucket_prefix'] + '/' if config['s3_bucket_prefix'] else ''

    print(f"  Bucket: {config['s3_bucket']}")
    print(f"  Prefix: '{prefix if prefix else '(root)'}' ")

    # Use paginator to retrieve all objects
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=config['s3_bucket'],
        Prefix=prefix
    )

    thirty_minutes_ago = datetime.now() - timedelta(minutes=30)
    recent_backups = []

    for page in page_iterator:
        for obj in page.get('Contents', []):
            if obj['LastModified'].replace(tzinfo=None) > thirty_minutes_ago:
                recent_backups.append(obj)

    print(f"  Found {len(recent_backups)} recent backup(s)")

    backup_files = []
    for obj in recent_backups:
        size_mb = obj['Size'] / 1024 / 1024
        print(f"  {obj['Key']} ({size_mb:.2f} MB)")
        backup_files.append({
            'key': obj['Key'],
            'size': obj['Size'],
            'size_mb': f"{size_mb:.2f}",
            'last_modified': obj['LastModified'].isoformat(),
        })

    return backup_files

def verify_backup_in_b2(config):
    """Verify backup files in Backblaze B2"""
    print("\nVerifying backup in Backblaze B2")

    print(f"  Bucket: {config['backblaze_bucket']}")
    print(f"  Endpoint: {config['backblaze_endpoint']}")

    # Construct endpoint URL (require HTTPS)
    endpoint = config['backblaze_endpoint'].strip()
    if endpoint.startswith('https://'):
        pass
    elif '://' in endpoint:
        raise ValueError("B2 endpoint must use HTTPS")
    else:
        endpoint = f"https://{endpoint}"

    # Create B2 client using S3-compatible API
    b2 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=config['backblaze_key_id'],
        aws_secret_access_key=config['backblaze_app_key'],
    )

    # Use paginator to retrieve all objects
    paginator = b2.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=config['backblaze_bucket']
    )

    thirty_minutes_ago = datetime.now() - timedelta(minutes=30)
    recent_backups = []

    for page in page_iterator:
        for obj in page.get('Contents', []):
            if obj['LastModified'].replace(tzinfo=None) > thirty_minutes_ago:
                recent_backups.append(obj)

    print(f"  Found {len(recent_backups)} recent backup(s)")

    backup_files = []
    for obj in recent_backups:
        size_mb = obj['Size'] / 1024 / 1024
        print(f"  {obj['Key']} ({size_mb:.2f} MB)")
        backup_files.append({
            'key': obj['Key'],
            'size': obj['Size'],
            'size_mb': f"{size_mb:.2f}",
            'last_modified': obj['LastModified'].isoformat(),
        })

    return backup_files

def disable_container(lightsail, service_name):
    """Disable container service"""
    print("\nDisabling container service to prevent restarts")

    lightsail.update_container_service(
        serviceName=service_name,
        isDisabled=True
    )

    print("  Container service disabled - no more restarts")

def delete_container(lightsail, service_name):
    """Delete container service"""
    print("\nDeleting container service")

    lightsail.delete_container_service(
        serviceName=service_name
    )

    print("  Container service deleted - billing stopped")

def handle_already_disabled(lightsail, s3, config, mode="backup"):
    """Handle case where container is already disabled"""
    print("\nContainer is already disabled.")

    if not config['destroy_on_completion']:
        return {
            'status': 'already_disabled',
            'message': 'Container service is already disabled'
        }

    if mode == "restore":
        print("DESTROY_ON_COMPLETION=true - deleting disabled restore service...")
        delete_container(lightsail, config['service_name'])
        return {
            'status': 'completed_and_deleted',
            'mode': mode,
            'message': 'Restore service was already disabled and has been deleted.',
        }

    print("DESTROY_ON_COMPLETION=true - checking for backup in S3...")
    backup_files = verify_backup_in_s3(s3, config)

    if not backup_files:
        print("WARNING: No backup files found in S3 - keeping service for investigation")
        return {
            'status': 'already_disabled',
            'message': 'Container service is already disabled'
        }

    print(f"Found {len(backup_files)} backup file(s) in S3 - deleting service...")
    delete_container(lightsail, config['service_name'])

    return {
        'status': 'completed_and_deleted',
        'backup_count': len(backup_files),
        'backup_files': backup_files,
        's3_bucket': config['s3_bucket'],
        'message': 'Service was already disabled. Backup verified and service deleted.'
    }

def handle_multiple_backup_runs(lightsail, config, backup_count, backup_files, mode="backup"):
    """Handle multiple backup or restore runs detected"""
    job_label = get_job_label(mode)
    print(f"\nALERT: Detected {backup_count} {job_label.lower()} runs - container is restarting!")

    if backup_files and config['destroy_on_completion']:
        print("Backup files found in S3 - deleting service to stop restarts...")
        delete_container(lightsail, config['service_name'])
        cleanup_status = 'deleted'
    else:
        print("Disabling service to stop restarts...")
        disable_container(lightsail, config['service_name'])
        cleanup_status = 'disabled'

    return {
        'status': 'stopped_restart_loop',
        'backup_runs_detected': backup_count,
        'backup_count': len(backup_files),
        'backup_files': backup_files,
        'cleanup_status': cleanup_status,
        'message': f'Container was restarting ({backup_count} runs). Service {cleanup_status}.',
    }

def check_job_completion_status(s3_backup_files, b2_backup_files, b2_enabled, mode):
    """Check if backup is complete based on S3 and B2 files"""
    if mode == "restore":
        print("\nRestore mode - completion verified via container logs (S3 upload check skipped)")
        return False

    if b2_enabled:
        # If B2 is configured, require backups in BOTH S3 and B2
        if s3_backup_files and b2_backup_files:
            print(f"\n Found {len(s3_backup_files)} backup(s) in S3")
            print(f" Found {len(b2_backup_files)} backup(s) in B2")
            print("  All backups completed successfully - proceeding with cleanup")
            return True
        elif s3_backup_files and not b2_backup_files:
            print(f"\n Found {len(s3_backup_files)} backup(s) in S3")
            print("No backups found in B2 yet - waiting for B2 sync to complete...")
        elif not s3_backup_files and b2_backup_files:
            print("\n No backups found in S3 yet")
            print(f" Found {len(b2_backup_files)} backup(s) in B2")
            print("  Waiting for S3 backup to complete...")
        else:
            print("\n No backups found in S3 or B2 yet")
    else:
        # If B2 is not configured, only check S3
        if s3_backup_files:
            print(f"\nFound {len(s3_backup_files)} recent backup(s) in S3!")
            print("  Backup completed successfully - proceeding with cleanup")
            return True

    return False

def check_backup_completion_status(s3_backup_files, b2_backup_files, b2_enabled):
    """Backward-compatible wrapper for backup completion checks."""
    return check_job_completion_status(s3_backup_files, b2_backup_files, b2_enabled, "backup")

def verify_backups(s3, config):
    """Verify backups in both S3 and B2 (if configured)"""
    s3_backup_files = verify_backup_in_s3(s3, config)

    # Check if B2 is configured
    b2_enabled = config['backblaze_key_id'] and config['backblaze_app_key'] and config['backblaze_bucket']
    b2_backup_files = []

    if b2_enabled:
        try:
            b2_backup_files = verify_backup_in_b2(config)
        except Exception as e:
            print(f"\nWARNING: Failed to verify B2 backup: {e}")
            print("  Continuing with S3 verification only...")

    return s3_backup_files, b2_backup_files, b2_enabled

def check_container_logs_and_status(lightsail, config, s3_backup_files, b2_backup_files, mode):
    """Check container logs for job completion, failure, and restart loops"""
    job_label = get_job_label(mode)
    print("\n" + "=" * 60)
    print(f"{job_label} not complete - checking container logs")
    print("=" * 60)
    print("\nFetching status logs (filtered)")
    log_events = get_job_status_logs(lightsail, config, mode)

    if log_events:
        print(f"  {len(log_events)} status lines...")
        for event in log_events[-15:]:
            print(f"    {event['message']}")
    else:
        print("  No status log lines found")

    failed, failure_reason = detect_job_failure(log_events, mode)
    if failed:
        print(f"\nERROR: Container logs indicate failure ({failure_reason})")
        print("Disabling container to stop restart loop...")
        disable_container(lightsail, config['service_name'])
        return 'failed', {
            'status': 'failed',
            'mode': mode,
            'failure_reason': failure_reason,
            'cleanup_status': 'disabled',
            'message': f'{job_label} failed ({failure_reason}). Container disabled.',
        }

    multiple_runs, job_count = detect_multiple_job_runs(log_events, mode)

    # Restore/backup success in the latest run: clean up even if older restarts exist.
    job_complete = is_job_complete(log_events, mode)
    print(f"\n{job_label} complete (from logs): {job_complete}")

    if job_complete:
        return 'logs_indicate_complete', None

    if multiple_runs:
        print(f"\nWARNING: Container has restarted {job_count} times!")
        print(f"  This usually means the first {job_label.lower()} finished and Lightsail kept restarting")
        print("  Disabling container to stop restart loop...")
        all_backup_files = s3_backup_files + b2_backup_files
        return 'restart_detected', handle_multiple_backup_runs(lightsail, config, job_count, all_backup_files, mode)

    return 'in_progress', None

def perform_cleanup_and_get_result(lightsail, s3, config, state=None, backup_files=None, mode="backup"):
    """Perform cleanup and return result"""
    job_label = get_job_label(mode)
    print(f"\n{job_label} completed!")

    # If backup_files not provided, verify in S3 (backward compatibility)
    if backup_files is None:
        backup_files = verify_backup_in_s3(s3, config)

    if config['destroy_on_completion']:
        print("\nDeleting container service...")
        delete_container(lightsail, config['service_name'])
        cleanup_status = 'deleted'
    else:
        print("\nDisabling container to prevent restarts (DESTROY_ON_COMPLETION=false)")
        disable_container(lightsail, config['service_name'])
        cleanup_status = 'disabled'

    result = {
        'status': 'completed',
        'mode': mode,
        'backup_count': len(backup_files),
        'backup_files': backup_files,
        's3_bucket': config['s3_bucket'],
        'cleanup_status': cleanup_status,
        'completed_at': datetime.now().isoformat(),
    }

    # Add B2 info if configured
    if config.get('backblaze_bucket'):
        result['backblaze_bucket'] = config['backblaze_bucket']

    if state:
        duration = calculate_duration(state["first_started_at"])
        result.update({
            'polling_attempts': state['polling_attempt'],
            'polling_duration_minutes': round(duration, 1),
            'first_started_at': state['first_started_at'],
        })

        print("\nPolling Statistics:")
        print(f"  Total attempts: {state['polling_attempt']}")
        print(f"  Total duration: {duration:.1f} minutes")

    print("\n" + "=" * 60)
    print(f"{job_label} Completed Successfully")
    print("=" * 60)
    if mode != "restore":
        print(f"Files Created: {result['backup_count']}")
        print(f"S3 Bucket: {result['s3_bucket']}")
        if config.get('backblaze_bucket'):
            print(f"B2 Bucket: {config['backblaze_bucket']}")
    print(f"Cleanup: {cleanup_status}")
    print("=" * 60)

    return result

def cleanup_on_timeout(lightsail, config, timeout_result):
    """Disable or delete container when polling times out."""
    try:
        if config['destroy_on_completion']:
            delete_container(lightsail, config['service_name'])
            timeout_result['cleanup_status'] = 'deleted'
        else:
            disable_container(lightsail, config['service_name'])
            timeout_result['cleanup_status'] = 'disabled'
    except Exception as error:
        timeout_result['cleanup_error'] = str(error)
        print(f"\nWARNING: Failed to clean up container after timeout: {error}")

    return timeout_result

def handler(pd):  # noqa: ARG001 pylint: disable=W0613
    """Monitor backup or restore progress with polling support"""
    config = load_config()

    # Debug: Print B2 config status
    print("DEBUG: B2 config loaded:")
    print(f"  B2_APPLICATION_KEY_ID: {'[SET]' if config.get('backblaze_key_id') else '[EMPTY]'}")
    print(f"  B2_APPLICATION_KEY: {'[SET]' if config.get('backblaze_app_key') else '[EMPTY]'}")
    print(f"  B2_BUCKET: {config.get('backblaze_bucket') or '[EMPTY]'}")
    print(f"  B2_HOST: {config.get('backblaze_endpoint') or '[EMPTY]'}")
    print()

    state = initialize_state(pd, config)
    mode = get_job_mode(state, config)
    job_label = get_job_label(mode)
    print(f"Mode: {mode}")

    timeout_result = check_timeout_conditions(state)
    if timeout_result:
        lightsail = boto3.client('lightsail', region_name=config['region'])
        return cleanup_on_timeout(lightsail, config, timeout_result)

    log_polling_status(state, mode)

    print(f"Service: {config['service_name']}")
    print(f"Container: {config['container_name']}")
    print()

    lightsail = boto3.client('lightsail', region_name=config['region'])
    s3 = boto3.client('s3', region_name=config['region'])

    try:
        print("Checking container status...")
        status = get_container_status(lightsail, config)
        print(f"  State: {status['state']}")
        print(f"  Disabled: {status['is_disabled']}")

        if status['state'] == 'NOT_FOUND':
            print("\nWARNING: Container service does not exist!")
            print("  This monitor workflow should only run after the deploy workflow creates the service.")
            return {
                'status': 'service_not_found',
                'service_name': config['service_name'],
                'message': 'Container service not found. Run deploy workflow first.',
            }

        if status['is_disabled']:
            return handle_already_disabled(lightsail, s3, config, mode)

        print("\n" + "=" * 60)
        print(f"Checking for completed {job_label.lower()}s")
        print("=" * 60)

        s3_backup_files = []
        b2_backup_files = []
        b2_enabled = False
        if mode != "restore":
            s3_backup_files, b2_backup_files, b2_enabled = verify_backups(s3, config)

        job_complete = check_job_completion_status(
            s3_backup_files, b2_backup_files, b2_enabled, mode
        )

        if job_complete:
            all_backup_files = s3_backup_files + b2_backup_files
            return perform_cleanup_and_get_result(
                lightsail, s3, config, state, all_backup_files, mode
            )

        log_status, result = check_container_logs_and_status(
            lightsail, config, s3_backup_files, b2_backup_files, mode
        )

        if log_status == 'failed':
            return result

        if log_status == 'restart_detected':
            return result

        if log_status == 'logs_indicate_complete':
            print(f"\nLogs indicate {job_label.lower()} completed - performing cleanup")
            return perform_cleanup_and_get_result(lightsail, s3, config, state, None, mode)

        print(f"\n{job_label} still in progress...")
        return schedule_next_poll_and_return(state, config, mode)

    except Exception as error:
        return handle_error(error, state)
