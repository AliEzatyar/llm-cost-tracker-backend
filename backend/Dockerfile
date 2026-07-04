# Step 1: Use an official lightweight Python image
FROM python:3.11-slim

# Step 2: Set environment variables for Python and Poetry paths
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=2.4.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

# Step 3: Explicitly append the Poetry home bin directory to system PATH globally
ENV PATH="$POETRY_HOME/bin:$PATH"

# Step 4: Set the working directory inside the container
WORKDIR /app

# Step 5: Install system tools, curl, and use the CORRECT raw installation script URL
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

# Step 6: Copy only configuration files to leverage Docker build cache
COPY pyproject.toml poetry.lock ./

# Step 7: Install dependencies ignoring development packages
RUN poetry install --no-interaction --no-ansi --no-root --only main

# Step 8: Copy the rest of your application code
COPY . .

# Step 9: Install the root project if it is packaged as a module
RUN poetry install --no-interaction --no-ansi --only main

# Step 10: Expose the application port
EXPOSE 8000

# Step 11: Run the application using Uvicorn
CMD ["poetry", "run", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
