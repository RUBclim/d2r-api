"""This wraps the terracotta flask app so we can add sentry error monitoring
and a proxy fix for the nginx reverse proxy. This is the entry point for the app.
"""
import os

import sentry_sdk
from psycopg2.errors import AdminShutdown
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from terracotta import get_settings
from terracotta import logs
from terracotta import update_settings
from terracotta.server import create_app
from werkzeug.middleware.proxy_fix import ProxyFix

from app import ALLOW_ORIGIN_REGEX


sentry_sdk.init(
    dsn=os.environ.get('SENTRY_DSN'),
    integrations=[FlaskIntegration(), SqlalchemyIntegration()],
    traces_sample_rate=float(os.environ.get('TC_SENTRY_SAMPLE_RATE', 0.0)),
    ignore_errors=[AdminShutdown],
)

settings = get_settings()
# we need to update some of the settings manually, since we can't reliably pass a
# regex via the environment variables for the CORS settings
update_settings(
    ALLOWED_ORIGINS_METADATA=[ALLOW_ORIGIN_REGEX],
    ALLOWED_ORIGINS_TILES=[ALLOW_ORIGIN_REGEX],
)

logs.set_logger(settings.LOGLEVEL, catch_warnings=True)

app = create_app(debug=settings.DEBUG, profile=settings.FLASK_PROFILE)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
