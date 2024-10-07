from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

router = APIRouter()


@router.get('/robots.txt', response_class=PlainTextResponse, include_in_schema=False)
def robots() -> str:
    data = 'User-agent: *\nDisallow: /\n'
    return data
