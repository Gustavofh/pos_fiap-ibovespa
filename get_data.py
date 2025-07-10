import io, os
import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()


session = boto3.Session(
  aws_access_key_id=os.getenv("aws_access_key_id"),
  aws_secret_access_key=os.getenv("aws_secret_access_key"),
  region_name=os.getenv("aws_region"),
)
s3 = session.client('s3')
prefix=os.getenv("prefix")
bucket=os.getenv("bucket")

# 2) Defina as chaves dos dois Parquets que você quer ler
keys = [
    'raw/codigo/pregao_diario.parquet',
    'raw/setor/pregao_diario.parquet',
]

for key in keys:
    print(f"Lendo s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    table = pq.read_table(io.BytesIO(data))
    df = table.to_pandas()
    print(df.head(), "\n")
