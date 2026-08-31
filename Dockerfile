FROM python:3.12-slim

# Download the latest installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Ensure installed binary is on the PATH
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

# Create directory for the data
RUN mkdir data

# Bronze data
RUN mkdir data/raw

# Trusted data
RUN mkdir data/trusted

# Delivery data (post transformation)
RUN mkdir data/delivered

# Copy the project into the image
COPY . . 

# Disable development dependencies
ENV UV_NO_DEV=1

# Sync the project to a project environment
RUN uv --version
RUN uv sync
