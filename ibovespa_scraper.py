import os
import io
import json
import base64
import datetime as dt

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from dotenv import load_dotenv

load_dotenv()

INDEX     = "IBOV"
LANG      = "pt-br"
PAGE_SIZE = 120
REF_DATE  = dt.date.today()

BUCKET    = os.getenv("bucket")
PREFIX    = os.getenv("prefix")
REGION    = os.getenv("aws_region")
ACCESS    = os.getenv("aws_access_key_id")
SECRET    = os.getenv("aws_secret_access_key")

session = boto3.Session(
    aws_access_key_id=ACCESS,
    aws_secret_access_key=SECRET,
    region_name=REGION
)
s3 = session.client("s3")

def brazil_number_to_float(txt: str) -> float:
    return float(txt.replace(".", "").replace(",", "."))

def fetch_segment(segment: int) -> list[dict]:
    """
    Busca JSON da B3 por segment (1=código, 2=setor).
    """
    payload = {
        "language": LANG,
        "pageNumber": 1,
        "pageSize": PAGE_SIZE,
        "index": INDEX,
        "segment": str(segment)
    }
    b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{b64}"
    
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])

def brazil_number_to_float(txt: str) -> float | None:
    try:
        return float(txt.replace(".", "").replace(",", "."))
    except:
        return None

for seg_val, seg_label in [(1, "codigo"), (2, "setor")]:
    print(f"\n🔍 Coletando segmento = {seg_label} …")
    rows = fetch_segment(seg_val)
    
    if not rows:
        print(f"⚠️ Sem dados para segmento '{seg_label}'")
        continue
    
    df = pd.DataFrame(rows)
    print(df.columns)

    df["ref_date"]     = REF_DATE
    df["consulta_por"] = seg_label
    
    cols = [
        ('segment',      'string'),
        ('cod',          'string'),
        ('asset',        'string'),
        ('type',         'string'),
        ('part',         'float'),
        ('partAcum',     'float'),
        ('theoricalQty', 'bigint'),
    ]

    for col, dtype in cols:
        if col not in df.columns:
            continue

        if dtype == 'string':
            df[col] = df[col].astype('string')

        elif dtype == 'float':
            # remove pontos de milhares e troca vírgula por ponto decimal
            df[col] = ( df[col]
                .str.replace(r'\.', '', regex=True)
                .str.replace(',', '.', regex=False)
                .astype('float64')
            )

        elif dtype == 'bigint':
            # remove pontos de milhares e converte pra inteiro nullable
            df[col] = ( df[col]
                .str.replace(r'\.', '', regex=True)
                .astype('Int64')
            )
    
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(
        table, buf,
        version="1.0",
        data_page_version="1.0",
        compression="snappy",
        coerce_timestamps="ms",
        use_deprecated_int96_timestamps=True
    )

    buf.seek(0)
    
    # key = f"{PREFIX}/consulta_por={seg_label}/ref_date={REF_DATE}/pregao_diario.parquet"
    key = f"{PREFIX}/{seg_label}/pregao_diario.parquet"
    s3.upload_fileobj(buf, BUCKET, key)
    print(f"✅ Enviado {len(df)} linhas para s3://{BUCKET}/{key}")
