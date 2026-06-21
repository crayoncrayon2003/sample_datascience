FROM python:3.12-slim
WORKDIR /app
# mlflow が起動時に git を探して出す警告を抑制(リポジトリ管理は不要なため)。
ENV GIT_PYTHON_REFRESH=quiet
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# train.py(学習 -> challenger 登録)と promote.py(昇格ゲート)の両方を同梱。
# 既定は train。promoter サービスは compose 側で command を promote.py に差し替える。
COPY train.py promote.py ./
CMD ["python", "train.py"]
