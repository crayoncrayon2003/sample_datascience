"""昇格ゲート(champion / challenger).

直近の学習で `@challenger` が指す version と、現役の `@champion` を ROC-AUC で比較し、
challenger が勝てば(または champion がまだ居なければ)`@champion` を challenger に
付け替える。負けたら据え置き(= 性能が下がるモデルは本番に出さない)。

ロールバックは「過去の version に champion alias を戻す」だけで完了する。
これが 004 までの『joblib を上書き』には無かった、version 管理の効果。

評価指標は各 version に紐づく run の `roc_auc` メトリクスを参照する(再計算しない)。
"""
import os
import sys

from mlflow import MlflowClient

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud")
CHAMPION_ALIAS = os.environ.get("CHAMPION_ALIAS", "champion")
CHALLENGER_ALIAS = os.environ.get("CHALLENGER_ALIAS", "challenger")
# challenger が同点のとき昇格させるか(同点なら新しい方を採用 = True 推奨)
PROMOTE_ON_TIE = os.environ.get("PROMOTE_ON_TIE", "true").lower() == "true"


def metric_for_alias(client: MlflowClient, alias: str):
    """alias が指す version とその roc_auc を返す。alias 不在なら (None, None)。"""
    try:
        mv = client.get_model_version_by_alias(MODEL_NAME, alias)
    except Exception:  # noqa: BLE001  (alias 未設定など)
        return None, None
    run = client.get_run(mv.run_id)
    return mv, run.data.metrics.get("roc_auc")


def main() -> None:
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    chal_mv, chal_auc = metric_for_alias(client, CHALLENGER_ALIAS)
    if chal_mv is None:
        print("[promote] challenger がいません。先に train を実行してください。")
        sys.exit(0)

    champ_mv, champ_auc = metric_for_alias(client, CHAMPION_ALIAS)

    chal_auc = chal_auc if chal_auc is not None else -1.0
    if champ_mv is not None and champ_mv.version == chal_mv.version:
        print(f"[promote] challenger と champion は同一 (v{chal_mv.version})。据え置き。")
        return

    if champ_mv is None:
        decision = True
        reason = "champion 不在(初回)"
    else:
        champ_auc = champ_auc if champ_auc is not None else -1.0
        if chal_auc > champ_auc or (PROMOTE_ON_TIE and chal_auc == champ_auc):
            decision = True
            reason = f"challenger {chal_auc} >= champion {champ_auc}"
        else:
            decision = False
            reason = f"challenger {chal_auc} < champion {champ_auc}"

    if decision:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, str(chal_mv.version))
        print(f"[promote] PROMOTE: {MODEL_NAME} v{chal_mv.version} -> @{CHAMPION_ALIAS} ({reason})")
    else:
        keep = champ_mv.version if champ_mv else "-"
        print(f"[promote] KEEP: champion=v{keep} のまま ({reason})")


if __name__ == "__main__":
    main()
