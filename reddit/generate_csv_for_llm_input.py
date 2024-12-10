import pandas as pd
from tqdm import tqdm

# Load and clean data
def load_and_clean_data(input_file: str) -> pd.DataFrame:
    """
    Load the dataset and clean it by removing rows with missing or deleted content.

    Args:
        input_file (str): Path to the input CSV file.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant columns.
    """
    df = pd.read_csv(input_file)
    df = df[['author', 'num_comments', 'selftext', 'title', 'ups', 'likes', 'created_utc']]
    df = df[~df['selftext'].isna()]
    df = df[~df['selftext'].isin(['[removed]', '[deleted]'])]
    return df

# Convert created_utc to year
def convert_to_year(date_val):
    """
    Convert a date value to the year. Handles Unix timestamps and datetime strings.

    Args:
        date_val: The date value to convert.

    Returns:
        int or None: The extracted year or None if parsing fails.
    """
    try:
        if isinstance(date_val, str):
            try:
                return pd.to_datetime(int(date_val), unit='s').year
            except ValueError:
                return pd.to_datetime(date_val).year
        return pd.to_datetime(date_val, unit='s').year
    except Exception:
        return None

def main():
    input_file = "submissions_combined.csv"
    output_subset_file = "posts_subset.csv"

    # Load and clean data
    print("Loading and cleaning data...")
    df = load_and_clean_data(input_file)

    # Convert created_utc to year with progress tracking
    print("Converting 'created_utc' to years...")
    tqdm.pandas(desc="Converting dates")
    df['year'] = df['created_utc'].progress_apply(convert_to_year)

    # Drop rows where year conversion failed
    df = df.dropna(subset=['year'])

    # Count posts per year
    print("\nTotal posts per year:")
    year_counts = df['year'].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"{year}: {count:,} posts")

    # Count posts with non-NaN upvotes per year
    print("\nPosts with upvotes per year:")
    year_counts_with_ups = df[df['ups'].notna()]['year'].value_counts().sort_index()
    for year, count in year_counts_with_ups.items():
        print(f"{year}: {count:,} posts")

    # Add unique IDs and extract a subset with 10k sample posts for each year for export.
    df['id'] = range(len(df))
    sampled_df = df.groupby('year', group_keys=False).apply(lambda x: x.sample(n=min(len(x), 10000)))

    # Save subset to CSV
    print(f"\nSaving subset of posts to '{output_subset_file}'...")
    sampled_df.to_csv(output_subset_file, index=False)
    print("Subset saved successfully.")

if __name__ == "__main__":
    main()
