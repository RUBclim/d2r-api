[![ci](https://github.com/RUBclim/d2r-api/actions/workflows/CI.yaml/badge.svg)](https://github.com/RUBclim/d2r-api/actions/workflows/CI.yaml)
[![pre-commit](https://github.com/RUBclim/d2r-api/actions/workflows/pre-commit.yaml/badge.svg)](https://github.com/RUBclim/d2r-api/actions/workflows/pre-commit.yaml)

# d2r-api

## installation

This is packaged and can be installed via:

via https

```bash
pip install git+https://github.com/RUBclim/d2r-api
```

via ssh

```bash
pip install git+ssh://git@github.com/RUBclim/d2r-api
```

## development

### setup the development environment

1. create a virtual environment using `tox` (needs to be available globally)
   ```bash
   tox --devenv venv -e py313
   ```
1. alternatively, create the virtual environment manually
   ```bash
   virtualenv venv -ppy313
   ```
   **or**
   ```bash
   python3.13 -m venv venv
   ```
   **or**
   ```bash
   uv venv venv -ppython313
   ```
1. and install the requirements
   ```bash
   pip install -r requirements.txt -r requirements-dev.txt
   ```
1. activate the virtual environment
   ```bash
   . venv/bin/activate
   ```
1. install and set up `pre-commit`. If not already installed globally, run
   ```bash
   pip install pre-commit
   ```
   setup the git-hook
   ```bash
   pre-commit install
   ```

### run only the web app

You can only run the web app without the queue and worker process

1. export the environment variables
   ```bash
   export $(cat .env.dev | grep -Ev "^#" | xargs -L 1)
   ```
1. start the database container
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.dev up -d db
   ```
1. run the web app
   ```bash
   DB_HOST=localhost uvicorn app.main:app --reload
   ```

### run the entire system in development mode

You need to have `docker compose` and `docker` installed on your system.

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.dev up -d
```

- the setup is configured, so that the fastapi web app restarts if changes are made to
  any of the Python code. The celery worker, however, needs to be restarted manually.

### run the tests

You can run the tests including `coverage` using `tox`

```bash
tox -e py
```

You can run the tests using only `pytest` (without coverage)

```
pytest tetsts/
```

#### run celery locally

Celery does not support auto-reloading. You can workaround that using the
[`watchdog`](https://pypi.org/project/watchdog/) package and start celery like this:

```bash
PGPORT=5433 TC_DATABASE_HOST=localhost CELERY_BROKER_URL=redis://localhost:6379/0 \
watchmedo auto-restart --directory=app --pattern=*.py --recursive -- \
celery -A app.tasks worker --concurrency=1
```

You may have to override additional environment variables, depending on what part you
are working on.

### upgrade requirements

We are using `uv pip compile` to manage our requirements

### backups

**for the network data**

A database backup/dump in production can be done by running these commands from the
host:

```bash
docker exec db pg_dump -Fc d2r_db -U dbuser > d2r-db.dump
```

This will generate some hypertable-related warnings, but they
[can be ignored](https://github.com/timescale/timescaledb/issues/1581).

The backup can be restored like this:

1. bring up a temporary `db` container and mount the backup you want to restore as a
   volume

   ```bash
   docker compose \
      -f docker-compose.yml \
      -f docker-compose.prod.yml \
      --env-file .env.prod \
      run \
      --rm \
      -v "$(pwd)/d2r_db.dump:/backups/d2r_db.dump:ro" \
      --name db \
      db
   ```

1. prepare the database for restore

   ```bash
   docker exec -it db psql -U dbuser -d d2r_db -c "SELECT timescaledb_pre_restore();"
   ```

1. perform the restore - this will take some time!

   ```bash
   docker exec -it db pg_restore -Fc -d d2r_db -U dbuser /backups/d2r_db.dump
   ```

1. finish the restore

   ```bash
   docker exec -it db psql -U dbuser -d d2r_db -c "SELECT timescaledb_post_restore();"
   ```

1. stop the temporary container

   ```bash
   docker stop db
   ```

1. start all services as usual

   ```bash
   docker compose \
      -f docker-compose.yml \
      -f docker-compose.prod.yml \
      --env-file .env.prod \
      up -d
   ```

**for the raster data**

A database backup/dump in production can be done by running these commands from the
host:

```bash
docker exec terracotta-db pg_dump -Fc terracotta -U dbuser
```

The backup can be restored like this:

1. restore the raster files to the correct directory by extracting them from restic (if
   stored there). This may already be the final destination (e.g. mounted via sshfs), if
   available, otherwise you will have to copy them from an intermediate directory to the
   final destination.

   ```bash
   restic -r d2r restore <ID of the backup> --target /tmp/
   ```

1. bring up a temporary `terracotta-db` container and mount the backup you want to
   restore as a volume

   ```bash
   docker compose \
      -f docker-compose.yml \
      -f docker-compose.prod.yml \
      --env-file .env.prod run \
      --rm \
      -v "$(pwd)/d2r_tc_db.dump:/backups/d2r_tc_db.dump:ro" \
      --name terracotta-db \
      terracotta-db
   ```

1. create a database to restore into

   ```bash
   docker exec -it terracotta-db psql -U dbuser -d postgres -c "CREATE DATABASE terracotta;"
   ```

1. perform the restore

   ```bash
   docker exec -it terracotta-db pg_restore -Fc -d terracotta -U dbuser /backups/d2r_tc_db.dump
   ```

1. stop the temporary container

   ```bash
   docker stop db
   ```

1. start all services as usual

   ```bash
   docker compose \
      -f docker-compose.yml \
      -f docker-compose.prod.yml \
      --env-file .env.prod \
      up -d
   ```
