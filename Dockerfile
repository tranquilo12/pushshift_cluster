FROM python:3.8-alpine as base

FROM base as builder
RUN apk update && apk add gcc g++ libc-dev
RUN mkdir /install

COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r requirements.txt

FROM base
COPY --from=builder /install /usr/local
COPY app /app
WORKDIR /app

ENV AWS_ACCESS_KEY_ID=AKIAQZFC5DKVP6GYWMYE
ENV AWS_SECRET_ACCESS_KEY=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

ENTRYPOINT ["python"]
CMD ["app.py"]