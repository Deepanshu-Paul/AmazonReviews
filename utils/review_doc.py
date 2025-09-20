import pandas as pd
import s3fs

def download_and_export_reviews(category: str, output_tsv: str, output_csv: str):
    """
    Download the Amazon Reviews TSV for the given category from the Public S3 bucket
    and export it as a CSV file.
    """
    s3_path = f"s3://amazon-reviews-pds/tsv/amazon_reviews_us_{category}_v1_00.tsv.gz"
    fs = s3fs.S3FileSystem(anon=True)
    
    # Read the compressed TSV directly from S3
    df = pd.read_csv(
        fs.open(s3_path, mode='rb'),
        sep='\t',
        compression='gzip',
        dtype={
            "reviewerID": str,
            "asin": str,
            "reviewerName": str,
            "helpful": str,
            "reviewText": str,
            "overall": float,
            "summary": str,
            "unixReviewTime": int,
            "reviewTime": str
        },
        parse_dates=["unixReviewTime"],
        nrows=100000  # adjust or remove for full dataset
    )
    
    # Optionally save the raw TSV locally
    df.to_csv(output_csv, index=False)
    print(f"Exported reviews to {output_csv}")

def download_and_export_metadata(output_csv: str):
    """
    Download the Amazon product metadata CSV from the Public S3 bucket
    and export it as a local CSV file.
    """
    s3_path = "s3://amazon-reviews-pds/tsv/metadata.csv.gz"
    fs = s3fs.S3FileSystem(anon=True)
    
    df = pd.read_csv(
        fs.open(s3_path, mode='rb'),
        compression='gzip',
        dtype={
            "asin": str,
            "title": str,
            "price": float,
            "imUrl": str,
            "brand": str,
            "category": str
        },
        nrows=100000  # adjust or remove for full dataset
    )
    
    df.to_csv(output_csv, index=False)
    print(f"Exported metadata to {output_csv}")

if __name__ == "__main__":
    # Replace 'Sports' with any other category (e.g., 'Books', 'Electronics')
    download_and_export_reviews(
        category="Sports",
        output_tsv="amazon_reviews_sports.tsv.gz",
        output_csv="amazon_reviews_sports.csv"
    )
    download_and_export_metadata(output_csv="amazon_product_metadata.csv")
