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
   tox --devenv venv -e py312
   ```
1. alternatively, create the virtual environment manually
   ```bash
   virtualenv venv -ppy312
   ```
   **or**
   ```bash
   python3.12 -m venv venv
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

### upgrade requirements

We are using `pip-tools` to manage our requirements
