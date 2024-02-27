FROM python:3.9-alpine

WORKDIR /app

COPY ../../pyproject.toml /app/
COPY ../../poetry.lock /app/
COPY /apps/search_api/server.py /app/

RUN apk add --no-cache curl && \
    curl -sSL https://install.python-poetry.org | python3 && \
    poetry config virtualenvs.create false

RUN poetry install --no-dev

# Change the command to run the Flask server, adjusting the path as necessary
CMD ["poetry", "run", "python", "apps/search_api/server.py"]


FROM python:3.9-alpine

# Set working directory
WORKDIR /app

# Copy necessary files
COPY ../../pyproject.toml /app/
COPY ../../poetry.lock /app/
COPY /apps/search_api/server.py /app/

# Install system dependencies required for Poetry and potential Python packages
RUN apk add --no-cache curl gcc musl-dev libffi-dev openssl-dev python3-dev make

RUN apk add --no-cache curl && \
    curl -sSL https://install.python-poetry.org | python3 && \
    poetry config virtualenvs.create false


# Install Python dependencies without dev dependencies
RUN poetry install --no-dev

# Adjust the command to run the Flask server
CMD ["poetry", "run", "python", "apps/search_api/server.py"]
