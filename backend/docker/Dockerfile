FROM python:3.11.3

# install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# curl uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app/backend

RUN mkdir data

COPY ../pyproject.toml ../uv.lock ./

COPY ../src ./src

COPY docker/entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh

RUN uv sync --frozen --no-dev

ENTRYPOINT ["/app/backend/entrypoint.sh"]
