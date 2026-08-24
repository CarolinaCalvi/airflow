FROM python:3.12-slim

WORKDIR /src

RUN pip install --no-cache-dir requests pandas pyarrow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]