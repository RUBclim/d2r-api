# Development

## Managing Requirements

### adding a new requirement

For production requirements add them to `requirements.in`, for development requirements
add them to `requirements-dev.in`. Then run for production requirements:

```bash
uv pip compile --no-annotate requirements.in -o requirements.txt
```

...and for development requirements

```bash
uv pip compile --no-annotate requirements-dev.in -o requirements-dev.txt
```

This will add the new requirements but will not upgrade all others

### upgrading existing requirements

To upgrade the existing production requirements run:

```bash
uv pip compile --upgrade --no-annotate requirements.in -o requirements.txt
```

...and the development requirements:

```bash
uv pip compile --upgrade --no-annotate requirements-dev.in -o requirements-dev.txt
```

This also done automatically once a week in a GitHub Actions workflow which creates a
PR.

## Database

### Views

The database implements its own manually-managed views so incremental refreshes are
supported. Only the most recent data is refreshed every five minutes. However, to make
the system self-healing, once a day all views are fully refreshed.

A code-generating tool was developed to generate hourly and daily views based on the raw
data. A pre-commit hook ensures the everything stays in sync (`/bin/generate_view.py`).

### Migrations

This system uses alembic for database migrations. Make sure you generate/implement a
migration for every change made to the database.

You can create a new migration by running:

```bash
alembic revision --autogenerate -m "<message_what_changed>"
```

If the database was just created by using the latest schema, you have to stamp it by
running:

```bash
alembic stamp head
```

```{warning}
Running migration can result in the loss of data. For example when an upgrade removes a
column, it will also remove the data. A downgrade, however, will only restore the column
but not the data previously stored in the column. This will have to be restored from a
backup.
```

You can upgrade the database for the latest schema by running:

```bash
alembic upgrade head
```

You can downgrade the database to the previous schema by running:

```bash
alembic downgrade -1
```

## Deployment

The deployment is implemented in ansible. The repository is private. The full
deployments workflow will be adapted upon transfer of the system to a new home.

It currently requires a (virtual) machine with the following specs

- 8x CPU
- 16 GB RAM
- 32 GB HDD for the OS
- 3 TB HDD for the data (the raster data is quite big. If only the measurement data API
  is needed, this can be much less. 1 year of data roughly equals 5 GB in the database,
  static data is around 1 GB)
- Debian-based OS (currently Ubuntu 24.04 - noble)

```bash
ansible-playbook d2r.yml
```

- you will be prompted for the become (sudo) password
- you will be prompted for the vault password.
  - the vault contains all secrets needed for the deployment
    - `.env.prod` file
    - SSL-Certificates

### Environment Variables

An example of all necessary environment variables can be found in `.env.dev` and may be
adapted for production use.

| Variable                 | Description                                                                                                                                                                                                                                                                                |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `DATA_DIRECTORY`         | Absolute path to the data directory **on the host machine**. This will be used to store the model output rasters and temperature/relative humidity interpolation rasters. For example `/data/rasters`.                                                                                     |
| `STATIC_DIRECTORY`       | Absolute path to the directory **on the host machine** where all static images for the `/v1/stations/metadata/<station_id>` route are stored. For example `/data/static`.                                                                                                                  |
| `DB_PROVIDER`            | Provider string of the database used. Since this uses timescale, only postgres-like engines are supported. For example `postgresql+psycopg` (psycopg 3).                                                                                                                                   |
| `DB_HOST`                | Host name of the database (container) usually corresponds to the name of the database container. In this case `db`.                                                                                                                                                                        |
| `PGPORT`                 | Port the database is listening on. For postgres this is usually `5432`.                                                                                                                                                                                                                    |
| `POSTGRES_USER`          | Database user to use to connect to the database. For example `dbuser`.                                                                                                                                                                                                                     |
| `POSTGRES_PASSWORD`      | Password of above `POSTGRES_USER`.                                                                                                                                                                                                                                                         |
| `POSTGRES_DB`            | Name of the database. If it does not exist, it is created using this name. For example `d2r_db`.                                                                                                                                                                                           |
| `SENTRY_DSN`             | Sentry data source name (DSN) [What the DSN Does](https://docs.sentry.io/product/sentry-basics/dsn-explainer/#what-the-dsn-does). This is optional, if not set, no errors are reported.                                                                                                    |
| `SENTRY_SAMPLE_RATE`     | Sentry traces sample rate (what % of transactions should be send to sentry [0 - 1]) [traces_sample_rate](https://docs.sentry.io/platforms/python/configuration/sampling/#configuring-the-transaction-sample-rate).                                                                         |
| `CELERY_BROKER_URL`      | URL to the celery broker/task queue that distributes tasks. This is usually `redis://redis:6379/0` since the container name is `redis`.                                                                                                                                                    |
| `QUEUE_SOFT_TIME_LIMIT`  | Time limit of a single task in seconds. If this is exceeded the task is killed. Usually `360`.                                                                                                                                                                                             |
| `ELEMENT_API_KEY`        | API-Key for the Element IoT platform operated by DOData. This will have to have read scope on the `Stadt Dortmund/Klimasensoren` folders.                                                                                                                                                  |
| `TC_DRIVER_PROVIDER`     | Database provider for the [terracotta](https://terracotta-python.readthedocs.io/en/latest/) instance. In this case also `postgresql`.                                                                                                                                                      |
| `TC_DATABASE_NAME`       | Name of the database for terracotta. If it does not exist, it is created using this name. For example `terracotta`.                                                                                                                                                                        |
| `TC_DATABASE_HOST`       | Host name of the database (container) usually corresponds to the name of the database container. In this case `terracotta-db`. You may also use the same db-container as above.                                                                                                            |
| `TC_SENTRY_SAMPLE_RATE`  | Sentry traces sample rate (what % of transactions should be send to sentry [0 - 1]) for the terracotta service [traces_sample_rate](https://docs.sentry.io/platforms/python/configuration/sampling/#configuring-the-transaction-sample-rate). The same `SENTRY_DSN` will be used for this. |
| `TC_RESAMPLING_METHOD`   | The resampling method to be used when reading reprojected raster data.                                                                                                                                                                                                                     |
| `TC_REPROJECTION_METHOD` | The resampling method to be used when reading reprojected raster data to web mercator.                                                                                                                                                                                                     |
| `NGINX_CERT_DIR`         | Directory **on the host machine**, where the SSL certificates are stored                                                                                                                                                                                                                   |
| `RASTER_LIFECYCLE_DAYS`  | The number of days (retention period) to keep model rasters on the machine and database. For example `30`.                                                                                                                                                                                 |
