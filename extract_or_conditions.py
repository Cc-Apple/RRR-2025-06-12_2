# extract_or_conditions.py
# ABTest=1 OR order_anomaly=1 の窓だけ抽出してCSV/JSON（必要ならPDF化は export_timeline_pdf の応用で可）
import pandas as pd
from pathlib import Path
import json

BASE = Path(__file__).resolve().parent.parent
BATCH_DIR = BASE / "Template2_FUKABORI_batches"
OUTDIR = BASE / "Template2_FUKABORI_outputs"
OUTDIR.mkdir(exist_ok=True, parents=True)

# flags CSV の収集
flag_files = sorted(BATCH_DIR.glob("batch_*/Template2_FUKABORI_30000_flags_*.csv"))
dfs = [pd.read_csv(f) for f in flag_files]
full_df = pd.concat(dfs, ignore_index=True)

# OR 抽出
or_df = full_df[(full_df.get("ABTest")==1) | (full_df.get("order_anomaly")==1)].copy()

# 出力
or_csv = OUTDIR/"FUKABORI_ABTestOROrderAnomaly.csv"
or_json = OUTDIR/"FUKABORI_ABTestOROrderAnomaly.json"
or_df.to_csv(or_csv, index=False)
or_df.to_json(or_json, orient="records", force_ascii=False, indent=2)
print("OK:", or_csv, or_json)
