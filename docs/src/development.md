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
data. A pre-commit hook ensures the everything stays in sync.

### Migrations

This system uses alembic for database migrations. Make sure you generate/implement a
migration for every change made to the database.

## Deployment

The deployment is implemented in ansible. The repository is private. The full
deployments workflow will be adapted upon transfer of the system to a new home.

It currently requires a machine with the following specs

- 8x CPU
- 16 GB RAM
- 32 GB HDD for the OS
- 3 TB HDD for the data
- Debian-based OS (currently Ubuntu 24.04 - noble)

```bash
ansible-playbook d2r.yml
```

- you will be prompted for the become (sudo) password
- you will be prompted for the vault password.
  - the vault contains all secrets needed for the deployment
    - `.env.prod` file
    - SSL-Certificates
