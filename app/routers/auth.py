from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Annotated

import jwt
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from fastapi.security import OAuth2PasswordBearer
from fastapi.security import OAuth2PasswordRequestFormStrict
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app import models
from app.database import get_db_session

router = APIRouter(prefix='/auth', tags=['auth'])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='auth/token')

# TODO: this all needs to be part of the config via env vars
SECRET_KEY = 'deadbeefcafe'
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 30


class Token(BaseModel):
    access_token: str
    token_type: str


class User(BaseModel):
    username: str
    disabled: bool


async def authenticate_user(
        db: AsyncSession,
        username: str,
        password: str,
) -> models.User | None:
    user = (
        await db.execute(select(models.User).where(models.User.username == username))
    ).scalars().one_or_none()

    if not user:
        return None
    if not user.verify_password(password):
        return None
    return user


def create_access_token(
        data: dict[str, str | datetime],
        expires_delta: timedelta,
) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({'exp': expire})
    encoded_jwt = jwt.encode(to_encode, key=SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
        db: Annotated[AsyncSession, Depends(get_db_session)],
        token: Annotated[str, Depends(oauth2_scheme)],
) -> User | None:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': 'Bearer'},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get('sub')
        if username is None:
            raise credentials_exception

    except InvalidTokenError:
        raise credentials_exception
    user = (
        await db.execute(
            select(models.User).where(models.User.username == username),
        )
    ).scalars().one_or_none()
    if user is None:
        raise credentials_exception
    return User(username=user.username, disabled=user.disabled)


async def get_current_active_user(
        current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    if current_user.disabled:
        raise HTTPException(status_code=400, detail='Inactive user')
    return current_user


@router.post('/token')
async def login_for_access_token(
        form_data: Annotated[OAuth2PasswordRequestFormStrict, Depends()],
        db: AsyncSession = Depends(get_db_session),
) -> Token:
    user = await authenticate_user(
        db=db,
        username=form_data.username,
        password=form_data.password,
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Incorrect username or password',
            headers={'WWW-Authenticate': 'Bearer'},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={'sub': user.username},
        expires_delta=access_token_expires,
    )
    return Token(access_token=access_token, token_type='bearer')
