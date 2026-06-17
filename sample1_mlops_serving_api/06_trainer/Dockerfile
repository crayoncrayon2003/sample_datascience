FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY train.py .

# 前処理済み parquet を読んでモデルを学習し /models に保存
CMD ["python", "train.py"]
