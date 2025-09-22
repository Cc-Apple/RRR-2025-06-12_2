# aggregate_timeline.py
# Template-2 FUKABORI: 各バッチの flags を集約し、triald/ABTest/variant/bucket のいずれかが True の行を抽出して
# タイムラインCSV/JSONを出力する。
import pandas as pd
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent  # adjust as needed
BATCH_DIR = BASE / "Template2_FUKABORI_batches"
OUTDIR = BASE / "Template2_FUKABORI_outputs"
OUTDIR.mkdir(exist_ok=True, parents=True)

# flags CSV の収集
flag_files = sorted(BATCH_DIR.glob("batch_*/Template2_FUKABORI_30000_flags_*.csv"))
dfs = []
for f in flag_files:
    try:
        dfs.append(pd.read_csv(f))
    except Exception:
        pass
full_df = pd.concat(dfs, ignore_index=True)

# 近接イベント（triald/ABTest/variant/bucket）抽出
cond_df = full_df[(full_df.get("triald")==True) | (full_df.get("ABTest")==True) | 
                  (full_df.get("variant")==True) | (full_df.get("bucket")==True)].copy()

# ざっくり timestamp 推定（file名から YYYY-MM-DD HH:MM:SS を拾う）
import re
TS_RE = re.compile(r"\b(20\d{2})[-/](\d{2})[-/](\d{2})[ T](\d{2}):(\d{2}):(\d{2})")
def extract_first_timestamp(x):
    m = TS_RE.search(str(x))
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"
    return None
cond_df["timestamp"] = cond_df["file"].apply(extract_first_timestamp)
cond_df = cond_df.sort_values(["timestamp","file"], ascending=[True,True])

# 出力
(OUTDIR/"Timeline_triald_ABTest.csv").write_text(cond_df.to_csv(index=False), encoding="utf-8")
cond_df.to_json(OUTDIR/"Timeline_triald_ABTest.json", orient="records", force_ascii=False, indent=2)
print("OK: timeline CSV/JSON saved in", OUTDIR)
