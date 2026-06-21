# MLflow Tracking Server + Model Registry。
#   - backend-store : PostgreSQL(experiments / runs / registered models / aliases)
#   - artifact-store : ローカルボリューム。--serve-artifacts でサーバが HTTP プロキシ
#     するので、Trainer / Scorer はボリューム共有なしに artifact を読み書きできる。
FROM python:3.12-slim

RUN pip install --no-cache-dir \
    mlflow==2.17.2 \
    psycopg2-binary==2.9.9

# backend-store-uri / artifacts-destination は compose の environment で上書きする。
ENV MLFLOW_BACKEND_STORE_URI=postgresql://app:app@postgres:5432/mlflow \
    MLFLOW_ARTIFACTS_DESTINATION=/mlflow/artifacts

EXPOSE 5000

# slim には curl/wget が無いので healthcheck は python で叩く。
HEALTHCHECK --interval=10s --timeout=5s --retries=12 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')" || exit 1

CMD ["sh", "-c", "mlflow server --host 0.0.0.0 --port 5000 \
    --backend-store-uri $MLFLOW_BACKEND_STORE_URI \
    --serve-artifacts --artifacts-destination $MLFLOW_ARTIFACTS_DESTINATION"]
