# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install system dependencies (cron)
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy requirements and install python dependencies
# (Create a requirements.txt with: pandas, requests, pyarrow, fastparquet, pytest)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create a shell script to run the pipeline
RUN echo '#!/bin/bash\npython /app/main.py >> /var/log/cron.log 2>&1' > /app/run_pipeline.sh
RUN chmod +x /app/run_pipeline.sh

# Add crontab file in the cron directory
# Run every day at 6:00 AM
RUN echo "0 6 * * * /app/run_pipeline.sh" > /etc/cron.d/my-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/my-cron

# Apply cron job
RUN crontab /etc/cron.d/my-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log
