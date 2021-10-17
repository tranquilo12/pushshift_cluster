FROM python:3.8-slim as base

FROM base as builder
# RUN apt update && apt add libgcc gcc g++ libc-dev
RUN mkdir /install

COPY requirements.txt /requirements.txt
RUN pip install --prefix="/install" -r requirements.txt

FROM base
COPY --from=builder /install /usr/local
COPY app /app
WORKDIR /app

ENV AWS_ACCESS_KEY_ID=AKIAQZFC5DKVMZ2YKOIZ
ENV AWS_SECRET_ACCESS_KEY=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

ENTRYPOINT ["python"]
CMD ["app.py"]