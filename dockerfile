# Use a lightweight official Python 3.11 image as the base
FROM python:3.11-slim

# Set the working directory inside the container
# All subsequent commands will run from this path
WORKDIR /usr/app

# Install required system packages:
# - git: needed for dbt to pull packages from repositories
# - build-essential: provides compilers (gcc, etc.) for building Python dependencies
# Clean up apt cache afterward to reduce image size
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version (recommended for compatibility and security)
# --no-cache-dir avoids storing installation cache, keeping the image smaller
RUN pip install --no-cache-dir --upgrade pip

# Install dbt core and the PostgreSQL adapter with specific versions
# Pinning versions ensures reproducible builds and avoids unexpected breaking changes
RUN pip install --no-cache-dir \
    dbt-core==1.11.6 \
    dbt-postgres==1.10.0

# Set the default command for the container
# This allows you to run dbt commands directly, e.g.:
#   docker run <image> run
# instead of:
#   docker run <image> dbt run
ENTRYPOINT ["dbt"]