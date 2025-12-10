FROM python:3.9-slim

WORKDIR /app

# install dep
RUN apt-get update && apt-get install -y libpq-dev gcc

# install req
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

# Checks
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]