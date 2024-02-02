FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim-2023-08-28

ENV APP_ENV=PROD

COPY requirements.txt /tmp/requirements.txt

RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./app /app/app