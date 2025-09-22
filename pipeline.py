# pipeline.py
# Consolidated core logic used in Room1 for Template-2 ZIP3 deep scan and exporters.

import re, zipfile, io
from pathlib import Path
import pandas as pd

WINDOW_HALF = 500_000     # ±500k
SLICE = 2_000             # 2k step
BATCH_SIZE = 100          # 100 hits per batch

JP_TERMS = ["認証","設定","追跡","許可","監視","共有","削除","確認","復元","遮断"]
JP_RE = re.compile("|".join(map(re.escape, JP_TERMS)))
SEAM_RE = re.compile(r"\s{200,}|(?:\n\s*){40,}")
TRIALD_RE = re.compile(r"triald", re.IGNORECASE)
ABTEST_RE = re.compile(r"ABTest", re.IGNORECASE)

def decode_best(b: bytes) -> str:
    for enc in ("utf-8","utf-8-sig","utf-16","utf-16le","utf-16be","latin-1"):
        try:
            return b.decode(enc, errors="ignore")
        except Exception:
            continue
    return b.decode("utf-8", errors="ignore")

def unicode_escape_twice(s: str) -> str:
    try:
        s = bytes(s,"utf-8").decode("unicode_escape")
        s = bytes(s,"utf-8").decode("unicode_escape")
    except Exception:
        pass
    return s

def strip_surrogates(s: str) -> str:
    if not isinstance(s,str):
        return s
    return re.sub(r"[\\ud800-\\udfff]", "", s)

def load_largest_text(zip_path: Path):
    text=None; file_id=None
    with zipfile.ZipFile(zip_path,"r") as z:
        members = [m for m in z.infolist() if not m.is_dir()]
        members.sort(key=lambda m:m.file_size, reverse=True)
        zi = members[0]
        file_id = f"{zip_path.name}:{zi.filename}"
        data = z.read(zi)
    text = unicode_escape_twice(decode_best(data))
    return text, file_id

def build_hitlist(text: str):
    hits = list(JP_RE.finditer(text))
    return pd.DataFrame([{"hit_index": i, "term": m.group(0), "start": m.start(), "end": m.end()} for i,m in enumerate(hits)])

def process_batch(text: str, hits, batch_index: int):
    summary_rows=[]; excerpt_rows=[]
    start_i = batch_index * BATCH_SIZE
    end_i = min(len(hits), start_i + BATCH_SIZE)
    for bi in range(start_i, end_i):
        m = hits[bi]
        s, e = m.start(), m.end()
        center = s
        w0 = max(0, center - WINDOW_HALF)
        w1 = min(len(text), center + WINDOW_HALF)
        total_span = w1 - w0
        n_slices = max(1, total_span // SLICE + (1 if total_span % SLICE else 0))
        for k in range(n_slices):
            a = w0 + k*SLICE
            b = min(w1, a + SLICE)
            seg = text[a:b]
            has_seam   = bool(SEAM_RE.search(seg))
            has_triald = bool(TRIALD_RE.search(seg))
            has_abtest = bool(ABTEST_RE.search(seg))
            summary_rows.append({
                "hit_index": bi, "slice_index": k, "slice_abs_start": a, "slice_abs_end": b,
                "offset_from_hit_start": a - s, "has_seam": has_seam, "has_triald": has_triald, "has_ABTest": has_abtest
            })
            if has_seam or has_triald or has_abtest:
                excerpt_rows.append({
                    "hit_index": bi, "slice_index": k,
                    "flags": ",".join([f for f,v in [("seam",has_seam),("triald",has_triald),("ABTest",has_abtest)] if v]) or "none",
                    "excerpt_2000": strip_surrogates(seg)
                })
    return pd.DataFrame(summary_rows), pd.DataFrame(excerpt_rows), start_i, end_i-1

def write_zip(zpath: Path, df_flags: pd.DataFrame, df_excerpt: pd.DataFrame, start_idx: int, end_idx: int):
    readme = f"Range: hits {start_idx}–{end_idx}\\nWindow: ±500,000\\nSlice: 2,000\\nExcerpts: flagged only\\n"
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.txt", readme)
        zf.writestr("SLICE_FLAGS.csv", df_flags.to_csv(index=False))
        zf.writestr("FLAGGED_EXCERPTS.csv", df_excerpt.to_csv(index=False))
    return zpath
