# TODO: this is not the best place for this, but we don't want any import side-effects
# when importing this to terracotta.
ALLOW_ORIGIN_REGEX = r'https?://([\w\-_]+\.)?(127\.0\.0\.1|localhost|geographie|data2resilience|data\-2\-resilience(\-[\w\-_]+\-vogelinos\-projects)?\.vercel)((\.rub)|(\.ruhr\-uni\-bochum))?(\.(de|app))?(:\d{2,4})?'  # noqa: E501
