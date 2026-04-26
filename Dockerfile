# Use an official Python runtime as a parent image
FROM python:3.10-slim as builder

# Set the working directory in the container
WORKDIR /app

# Install uv
RUN pip install uv

# Copy the project files into the container
COPY requirements.txt ./

# Install dependencies using uv
RUN uv pip install --system --no-cache -r requirements.txt

# Final stage
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the content of the local src directory to the working directory
COPY mcp_amundsen mcp_amundsen
COPY run.py .

# Specify the command to run on container start
CMD ["uvicorn", "run:app", "--host", "0.0.0.0", "--port", "8000"]
