import pandas as pd
import os

def transform_data(df):
    """
    Selecciona columnas útiles, limpia y normaliza datos.
    Maneja correctamente diccionarios ya existentes y strings.
    """

    # === Selección de columnas útiles ===
    df_clean = df[['id', 'name', 'genres', 'status', 'language', 'premiered', 'rating', 'network']].copy()

    # === Funciones auxiliares ===
    def parse_rating(x):
        """Extrae promedio de rating, maneja dict, string o nulo"""
        if pd.isnull(x):
            return None
        elif isinstance(x, dict):
            return x.get('average')
        elif isinstance(x, str):
            import ast
            try:
                return ast.literal_eval(x).get('average')
            except (ValueError, SyntaxError):
                return None
        else:
            return None

    def parse_network(x):
        """Extrae nombre de network, maneja dict, string o nulo"""
        if pd.isnull(x):
            return "Unknown"
        elif isinstance(x, dict):
            return x.get('name', "Unknown")
        elif isinstance(x, str):
            import ast
            try:
                return ast.literal_eval(x).get('name', "Unknown")
            except (ValueError, SyntaxError):
                return "Unknown"
        else:
            return "Unknown"

    # === Aplicar parsing ===
    df_clean['rating'] = df_clean['rating'].apply(parse_rating)
    df_clean['network'] = df_clean['network'].apply(parse_network)

    # === Limpieza y manejo de duplicados ===
    df_clean['network'] = df_clean['network'].fillna("Unknown")
    df_clean = df_clean.drop_duplicates(subset=['id'])

    # === Normalización ===
    df_clean['premiered'] = pd.to_datetime(df_clean['premiered'], errors='coerce')
    df_clean['rating'] = pd.to_numeric(df_clean['rating'], errors='coerce')
    df_clean['status'] = df_clean['status'].str.capitalize()
    df_clean['language'] = df_clean['language'].str.capitalize()

    print(f"Transformación completada: {len(df_clean)} filas procesadas")
    return df_clean
