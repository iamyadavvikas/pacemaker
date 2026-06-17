# Live governor dashboard + looping synthetic backfill.
# Build:  docker build -t pacing-governor .
# Run:    docker run --rm -p 8765:8765 -e GOV_DASH_HOST=0.0.0.0 \
#               -e GOV_DEMO_DSN=postgresql://gov:govpass@db:5432/govdemo pacing-governor
#
# Synthetic data only. The dashboard is unauthenticated; keep it on a private
# network and gate it before exposing publicly.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    GOV_DASH_HOST=0.0.0.0 \
    GOV_DASH_PORT=8765

WORKDIR /app

# Install deps first (better layer caching), then the package itself.
COPY pyproject.toml README.md ./
COPY src ./src
COPY harness ./harness
RUN pip install .

EXPOSE 8765

# Looping governed backfill + live dashboard. Override --mode/--workers as needed.
CMD ["python", "-m", "harness.live", "--no-open"]
