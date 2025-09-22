import requests
import pandas as pd
import os

def extract(max_pages=5):
    all_shows = []
    for page in range(max_pages):
        url = f"https://api.tvmaze.com/shows?page={page}"
        resp = requests.get(url)
        all_shows.extend(resp.json())

    df = pd.DataFrame(all_shows)

    # Ruta absoluta relativa al archivo actual
    # Los guarda en un CSV Local pero lo normal es guardarlo en S3
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "../../data/raw")
    os.makedirs(output_dir, exist_ok=True)

    # df.to_csv(os.path.join(output_dir, "shows_raw.csv"), index=False)

    return df

if __name__ == "__main__":
    extract()
