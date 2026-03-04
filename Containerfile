FROM debian:bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    GDAL_DATA=/usr/share/gdal \
    PROJ_LIB=/usr/share/proj \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# System deps: geospatial libs, mapshaper, git-lfs
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    git-lfs \
    gdal-bin \
    nodejs \
    npm && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    npm install -g mapshaper && \
    ln -sf /usr/local/bin/mapshaper /usr/local/bin/mapshaper-xl && \
    git lfs install --system

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create non-root user
RUN groupadd --gid 1000 gbbot && \
    useradd --uid 1000 --gid 1000 --create-home gbbot

# Install Python + deps as non-root
# uv reads requires-python from pyproject.toml to pick the Python version
WORKDIR /app
RUN chown gbbot:gbbot /app
USER gbbot

COPY --chown=gbbot:gbbot pyproject.toml uv.lock .python-version ./
RUN uv python install && uv sync --locked --no-dev --group builder

# Remove .venv before copying app files so local .venv can't overwrite it
# Then restore from the layer cache
COPY --chown=gbbot:gbbot builder builder

ENV PATH="/app/.venv/bin:$PATH"
