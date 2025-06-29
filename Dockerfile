# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
      ffmpeg \
      libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy & install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the PANNs class labels CSV into the path panns_inference expects
COPY panns_data /root/panns_data

# Copy application code
COPY . .

# Expose the port FastAPI will listen on
EXPOSE 8000

# Launch the app with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]