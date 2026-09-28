FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY monitor.py .
# 常駐: 一定間隔でドリフト/品質を計算して ClickHouse に書き込む
CMD ["python", "monitor.py"]
