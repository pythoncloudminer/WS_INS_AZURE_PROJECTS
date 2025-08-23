import logging
import os
import tempfile
from typing import Dict, List

import azure.functions as func
import pandas as pd
import pyexcel as pe
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = func.FunctionApp()


@app.event_grid_trigger(arg_name="event")
def scqt_cleanser(event: func.EventGridEvent):
    """Azure Event Grid Trigger function that processes Excel files from blob events."""
    try:
        # Event Grid event -> extract blob URL
        event_data = event.get_json()
        blob_url = event_data["url"]
        logger.info(f"Event received for blob: {blob_url}")

        # Get blob client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("AzureWebJobsStorage")
        )
        blob_client = blob_service_client.get_blob_client(blob_url)

        blob_name = os.path.basename(blob_url)
        if not is_excel_file(blob_name):
            logger.info(f"Skipping non-Excel file: {blob_name}")
            return

        # Temp directory path
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, blob_name)

        # Download blob to temp file
        with open(temp_file_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

        # Process file
        process_excel_file(temp_file_path, blob_name)

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        raise


def is_excel_file(filename: str) -> bool:
    """Check if the file is an Excel file."""
    return filename.lower().endswith(('.xls', '.xlsx'))


def process_excel_file(temp_file_path: str, blob_name: str):
    """Open Excel, detect tables, convert to CSV, and save to blob storage."""
    try:
        # Load file with pyexcel (works for both xls and xlsx)
        book = pe.get_book(file_name=temp_file_path)

        for sheet_name in book.sheet_names():
            logger.info(f"Processing worksheet: {sheet_name}")
            sheet = book[sheet_name]

            # Convert sheet to DataFrame
            df = pd.DataFrame(sheet.to_array())

            # Detect header/data
            table = detect_table_in_dataframe(df)
            if not table:
                logger.info(f"No table found in worksheet: {sheet_name}")
                continue

            df = table_to_dataframe(table)
            df = transform_dataframe(df, sheet_name, blob_name)

            save_dataframe_to_blob(
                df=df,
                original_blob_name=blob_name,
                sheet_name=sheet_name
            )

    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def detect_table_in_dataframe(df: pd.DataFrame) -> Dict[str, List]:
    """Detect table from DataFrame."""
    if df.empty:
        return {}

    num_cols = df.shape[1]

    # Find header row
    header_row = None
    for idx, row in df.iterrows():
        non_empty = row.notna().sum()
        if non_empty >= 0.8 * num_cols:
            header_row = idx
            break
    if header_row is None:
        return {}

    # Find end row
    end_row = len(df) - 1
    for idx in range(header_row + 1, len(df)):
        empty = df.iloc[idx].isna().sum()
        if empty >= 0.8 * num_cols:
            end_row = idx - 1
            break
    if end_row <= header_row:
        return {}

    # Extract headers
    headers = [
        str(val) if pd.notna(val) else f"Column_{i+1}"
        for i, val in enumerate(df.iloc[header_row])
    ]

    # Extract data
    data = df.iloc[header_row+1:end_row+1].values.tolist()

    return {"headers": headers, "data": data}


def table_to_dataframe(table: Dict[str, List]) -> pd.DataFrame:
    """Convert detected table to pandas DataFrame."""
    return pd.DataFrame(table['data'], columns=table['headers'])


def transform_dataframe(df: pd.DataFrame, sheet_name: str, filename: str) -> pd.DataFrame:
    """Transform DataFrame with additional metadata."""
    df.columns = [col.strip() for col in df.columns]
    df['source_file'] = filename
    df['source_sheet'] = sheet_name
    return df


def save_dataframe_to_blob(df: pd.DataFrame, original_blob_name: str, sheet_name: str):
    """Save DataFrame to blob storage as CSV."""
    original_name = os.path.splitext(os.path.basename(original_blob_name))[0]
    output_blob_name = f"processed/{original_name}_{sheet_name}.csv"

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AzureWebJobsStorage")
    )

    csv_data = df.to_csv(index=False).encode("utf-8")
    blob_client = blob_service_client.get_blob_client(
        container="output",
        blob=output_blob_name
    )

    blob_client.upload_blob(csv_data, overwrite=True)
    logger.info(f"Uploaded CSV to blob: {output_blob_name}")
