# Setup

## 1. Prerequisites

Before you begin, prepare:

- A Pipedream account.
- An AWS account with access to Lightsail container services.
- An S3 bucket for backup objects.
- Database network access from AWS Lightsail. Allow the database port from the network path used by the Lightsail container, and do not expose the database more broadly than necessary.

## 2. IAM sketch

Create an IAM principal for the workflows with only the permissions they need:

- Lightsail container APIs to get, create, update, deploy, read logs from, disable, and delete the configured container service.
- S3 permissions to put backup objects and list objects in the configured bucket and prefix.

Scope resources and actions as narrowly as AWS supports, keep credentials out of source control, and follow least privilege. A restore image may also need permission to read backup objects from S3.

## 3. Create the Monitor workflow in Pipedream

1. Create a new Pipedream workflow with an HTTP / Webhook trigger.
2. Add a Delay step and set it to the polling interval you plan to use (the template uses 60 seconds).
3. Add a Python code step and paste the contents of [`workflows/monitor/code/entry.py`](../workflows/monitor/code/entry.py).
4. Deploy the workflow.
5. Copy its HTTP webhook URL. You will use this URL for both `MONITOR_WEBHOOK_URL` and the Deploy workflow's HTTP POST step.

## 4. Create the Deploy workflow

1. Create another Pipedream workflow with a schedule trigger and choose your backup schedule.
2. Add a Python code step and paste the contents of [`workflows/deploy/code/entry.py`](../workflows/deploy/code/entry.py).
3. Add a Delay step after the Python step. The template uses five minutes so the container can start and run before monitoring begins.
4. Add an HTTP POST step after the delay.
5. Set the POST URL to the Monitor workflow URL you copied.
6. Send the Python step's `initial_state` return value as the JSON request body.
7. Deploy the workflow.

The reference step order and properties are available in [`workflows/deploy/workflow.yaml`](../workflows/deploy/workflow.yaml) and [`workflows/monitor/workflow.yaml`](../workflows/monitor/workflow.yaml).

## 5. Set environment variables

Copy the names and example values from [`.env.example`](../.env.example) into Pipedream environment variables. Replace every placeholder with your own value.

Choose one database configuration:

- **Postgres:** use the Postgres block, including `DB_PORT=5432` and `PGDATABASE=postgres`, and select a compatible Postgres dump/restore image.
- **MySQL:** use the commented MySQL alternate with `DB_PORT=3306` and a MySQL image that implements the environment contract expected by that image. Postgres-only variables are ignored by a MySQL image.

Set `MONITOR_WEBHOOK_URL` to the Monitor workflow's HTTP trigger URL. Leave optional B2 and Sentry variables unset if you do not use those services.

## 6. Run the first backup

1. Set `MODE=backup`.
2. Trigger the Deploy workflow manually.
3. Watch the Deploy and Monitor workflow logs for errors.
4. Confirm that a new backup object appears in the configured S3 bucket and prefix.
5. Confirm that the Lightsail container service is deleted when `DESTROY_ON_COMPLETION=true`, or disabled when it is `false`.

Do not rely on the schedule until this manual run completes successfully.

## 7. Restore notes

Set `MODE=restore`, select the backup expected by your dump/restore image, and trigger the Deploy workflow manually. Confirm the target database and credentials before running: restore mode can replace existing data. The Monitor workflow detects restore completion from container logs and then deletes or disables the service according to `DESTROY_ON_COMPLETION`.

Return `MODE` to `backup` before re-enabling the normal backup schedule.

## 8. Using the GitHub Template

1. On GitHub, select **Use this template** to create your own repository.
2. Clone your new repository.
3. Follow the steps above to create the Monitor and Deploy workflows, configure environment variables, and verify the first backup.
