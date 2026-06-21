-- MLflow Tracking / Model Registry のバックエンドストア用 DB。
-- OLTP(oltp)とは別 DB にして、MLflow が自分でテーブルを作成・マイグレーションする。
-- この init は初回(空ボリューム)のみ実行される。
CREATE DATABASE mlflow OWNER app;
