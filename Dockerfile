FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install fastapi uvicorn psycopg2-binary sqlalchemy python-dotenv

# Copy files
COPY models_pricing/ ./models_pricing/
COPY app.py ./
COPY .env ./

# Expose port
EXPOSE 8080

# Run the web server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]