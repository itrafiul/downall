# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install system dependencies (FFmpeg is required for video processing)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
# (Render will provide the PORT env var, which the app will use)
EXPOSE 8080

# Run app.py when the container launches
CMD ["python", "app.py"]
