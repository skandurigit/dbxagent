FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
RUN useradd --create-home --uid 1001 agent
COPY --from=builder /install /usr/local
WORKDIR /app
COPY --chown=agent:agent *.py ./
RUN mkdir -p /var/log && chown agent:agent /var/log
USER agent
EXPOSE 8080
ENTRYPOINT ["python", "main.py"]
