from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from fastapi.responses import RedirectResponse

router = APIRouter()


@router.get('/robots.txt', response_class=PlainTextResponse, include_in_schema=False)
def robots() -> str:
    data = 'User-agent: *\nDisallow: /\n'
    return data


@router.get('/', response_class=RedirectResponse, include_in_schema=False)
def index() -> RedirectResponse:
    """redirect requests to the index to the docs"""
    return RedirectResponse('/docs', status_code=301)
