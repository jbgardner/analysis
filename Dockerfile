# Use the official Python 3.10 image based on Alpine Linux as a development base
FROM python:3.10-alpine as development

# Expose port 8000 for the application
EXPOSE 8000

# Set environment variables to control Python behavior
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy the requirements file and install dependencies
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt

# Create the app directory and set it as the working directory
WORKDIR /app

# Copy the application code into the container
COPY . /app

# Command to run the application with Uvicorn
CMD ["uvicorn", "app:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]
