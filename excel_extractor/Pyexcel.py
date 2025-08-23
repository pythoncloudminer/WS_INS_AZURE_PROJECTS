import logging
import os
import json
import tempfile
from typing import Dict, List

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyexcel as pe   # <--- unified engine for both xls & xlsx

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

        # Download blob to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(blob_name)[1]) as tmp_file:
            tmp_file.write(blob_client.download_blob().readall())
            temp_file_path = tmp_file.name

        process_excel_file(temp_file_path, blob_name)

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        raise


def is_excel_file(filename: str) -> bool:
    """Check if the file is an Excel file."""
    return filename.lower().endswith(('.xls', '.xlsx'))


def process_excel_file(temp_file_path: str, blob_name: str):
    """Read Excel file using pyexcel, convert to CSV, and save to blob storage."""
    try:
        # Load the whole Excel file into a pyexcel Book (multiple sheets)
        book = pe.get_book(file_name=temp_file_path)

        for sheet_name in book.sheet_names():
            logger.info(f"Processing worksheet: {sheet_name}")
            sheet = book[sheet_name]

            # Convert sheet to pandas DataFrame
            df = pd.DataFrame(sheet.to_array()[1:], columns=sheet.to_array()[0])
            df = transform_dataframe(df, sheet_name, blob_name)

            save_dataframe_to_blob(
                df=df,
                original_blob_name=blob_name,
                sheet_name=sheet_name
            )

    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def transform_dataframe(df: pd.DataFrame, sheet_name: str, filename: str) -> pd.DataFrame:
    """Transform DataFrame with additional metadata."""
    df.columns = [str(col).strip() for col in df.columns]
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

    csv_data = df.to_csv(index=False).encode('utf-8')
    blob_client = blob_service_client.get_blob_client(
        container="output",
        blob=output_blob_name
    )

    blob_client.upload_blob(csv_data, overwrite=True)
    logger.info(f"Uploaded CSV to blob: {output_blob_name}")
