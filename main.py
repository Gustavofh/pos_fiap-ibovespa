from src.ibovespa_scraper import fetch_ibovespa_table
from src.functions import adj_dataframe_by_codigo, adj_dataframe_by_setor


df_codigo = fetch_ibovespa_table()
df_codigo = adj_dataframe_by_codigo(df_codigo)
print(df_codigo)
print(df_codigo.columns)
print(df_codigo.dtypes)


df_setor = fetch_ibovespa_table("Setor de Atuação")
df_setor = adj_dataframe_by_setor(df_setor)
print(df_setor)
print(df_setor.columns)