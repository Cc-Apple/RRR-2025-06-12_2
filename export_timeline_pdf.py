# export_timeline_pdf.py
# Template-2 FUKABORI: Timeline_triald_ABTest.csv をPDFの表にする。
import pandas as pd
from pathlib import Path
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

BASE = Path(__file__).resolve().parent.parent
IN_CSV = BASE/"Template2_FUKABORI_outputs"/"Timeline_triald_ABTest.csv"
OUT_PDF = BASE/"Template2_FUKABORI_outputs"/"Timeline_triald_ABTest.pdf"
OUT_PDF.parent.mkdir(exist_ok=True, parents=True)

df = pd.read_csv(IN_CSV)

styles = getSampleStyleSheet()
doc = SimpleDocTemplate(str(OUT_PDF), pagesize=landscape(A4))
elems = [Paragraph("Timeline of triald / ABTest / variant / bucket", styles["Heading2"]), Spacer(1,12)]

headers = ["timestamp","file","hit_term","triald","ABTest","variant","bucket","order_anomaly"]
data = [headers] + [[str(df.get(h)[i]) for h in headers] for i in range(len(df))]

table = Table(data, repeatRows=1)
table.setStyle(TableStyle([
    ("BACKGROUND",(0,0),(-1,0),colors.grey),
    ("TEXTCOLOR",(0,0),(-1,0),colors.whitesmoke),
    ("ALIGN",(0,0),(-1,-1),"CENTER"),
    ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
    ("FONTSIZE",(0,0),(-1,0),9),
    ("FONTSIZE",(0,1),(-1,-1),7),
    ("GRID",(0,0),(-1,-1),0.25,colors.black),
]))
elems.append(table)
doc.build(elems)
print("OK:", OUT_PDF)
