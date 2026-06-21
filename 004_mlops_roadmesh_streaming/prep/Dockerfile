# osmnx で道路網を取得する prep 専用イメージ(重いので runtime とは分離)
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir osmnx==1.9.4
COPY prepare.py .
CMD ["python", "prepare.py"]
