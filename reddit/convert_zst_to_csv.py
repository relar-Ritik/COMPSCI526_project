import pandas as pd
import zstandard as zstd
import json
import io

def convert_zst_to_csv(input_zst_path: str, output_csv_path: str) -> pd.DataFrame:
    """
    Convert a .zst compressed JSON file to a CSV file.

    Args:
        input_zst_path (str): Path to the input .zst file.
        output_csv_path (str): Path to save the output CSV file.

    Returns:
        pd.DataFrame: The resulting DataFrame after conversion.
    """
    records = []

    with open(input_zst_path, 'rb') as file:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in text_stream:
                try:
                    record = json.loads(line)
                    records.append(record)
                except json.JSONDecodeError:
                    continue

    df = pd.DataFrame(records)
    if 'created_utc' in df.columns:
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', utc=True)
    
    df.to_csv(output_csv_path, index=False)
    print(f"Successfully converted {input_zst_path} to {output_csv_path}")
    return df

if __name__ == "__main__":
    input_file = "SuicideWatch_submissions.zst"
    output_csv_file = "submissions.csv"
    convert_zst_to_csv(input_file, output_csv_file)
