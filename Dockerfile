# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory to /app
WORKDIR /app

# Install Google Cloud SDK
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.tar.gz && \
    tar -zxvf google-cloud-sdk.tar.gz && \
    ./google-cloud-sdk/install.sh

# Add the Cloud SDK tools to the path
ENV PATH $PATH:/app/google-cloud-sdk/bin

# Copy the current directory contents into the container at /app
COPY . /app

# Authenticate with Google Cloud
RUN gcloud auth activate-service-account --key-file=/app/cloud-storage.json

# Download the 'location.shp' file from Google Cloud Storage to /app
RUN gsutil cp gs://gis_regions/location.shp /app/location.shp

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Install python-multipart
RUN pip install python-multipart

# Make port 80 available to the world outside this container
EXPOSE 8008

# Run app.py when the container launches
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008"]
