import pandas as pd


def adj_dataframe_by_codigo(df: pd.DataFrame):
    
    df_renamed: pd.DataFrame = df.rename(
        columns={
            "Código": "codigo",
            "Ação": "acao",
            "Tipo": "tipo",
            "Qtde. Teórica": "qtd_teorica",
            "Part. (%)": "participacao"
        })
    
    df_renamed["qtd_teorica"] = (
        df_renamed["qtd_teorica"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .replace(",", "", regex=False)
        .astype("int")
    )
    
    df_renamed["participacao"] = (
        df_renamed["participacao"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype("float")
    )

    return df_renamed


def adj_dataframe_by_setor(df: pd.DataFrame):
    
    df_renamed: pd.DataFrame = df.rename(
        columns={
            "Setor": "setor",
            "Código": "codigo",
            "Ação": "acao",
            "Tipo": "tipo",
            "Qtde. Teórica": "qtd_teorica",
            "%Setor": "participacao",
            "Part. (%)": "participacao_acum",
        })
    
    df_renamed["qtd_teorica"] = (
        df_renamed["qtd_teorica"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .replace(",", "", regex=False)
        .astype("int")
    )
    
    df_renamed["participacao"] = (
        df_renamed["participacao"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype("float")
    )
    
    df_renamed["participacao_acum"] = (
        df_renamed["participacao_acum"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype("float")
    )

    return df_renamed