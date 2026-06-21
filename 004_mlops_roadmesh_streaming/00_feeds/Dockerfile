FROM python:3.12-slim
WORKDIR /app
# feed.py は標準ライブラリのみで動く(依存インストール不要)。
COPY congestion.py graphml_road.py feed.py ./
CMD ["python", "feed.py"]
