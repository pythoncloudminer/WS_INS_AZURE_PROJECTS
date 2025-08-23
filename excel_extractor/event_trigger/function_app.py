import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from table_extractor import ExcelTableExtractor
import os
import json

# Initialize the extractor
extractor = ExcelTableExtractor()

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="event")
def ExcelTableExtractorFunction(event: func.EventGridEvent):
    """
    Azure Function triggered by Blob Storage events to extract tables from Excel files
    """
    logging.info("Python EventGrid trigger function processed an event.")
    
    try:
        # Parse the event data
        event_data = event.get_json()
        logging.info(f"Event data: {json.dumps(event_data)}")
        
        # Extract blob information from the event
        blob_url = event_data.get('url')
        if not blob_url:
            logging.error("No blob URL found in event data")
            return
        
        # Get blob name and container from URL
        from urllib.parse import urlparse
        parsed_url = urlparse(blob_url)
        blob_path = parsed_url.path
        
        # Extract container and blob name
        path_parts = blob_path.split('/')
        if len(path_parts) < 3:
            logging.error(f"Invalid blob URL format: {blob_url}")
            return
        
        container_name = path_parts[1]
        blob_name = '/'.join(path_parts[2:])
        
        # Check if file is Excel
        file_extension = os.path.splitext(blob_name)[1].lower()
        if file_extension not in ['.xls', '.xlsx']:
            logging.info(f"Skipping non-Excel file: {blob_name}")
            return
        
        # Get storage connection string
        connection_string = os.environ.get('AzureWebJobsStorage')
        if not connection_string:
            logging.error("AzureWebJobsStorage connection string not found")
            return
        
        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get input blob container and blob
        input_container_client = blob_service_client.get_container_client(container_name)
        blob_client = input_container_client.get_blob_client(blob_name)
        
        # Download the Excel file
        logging.info(f"Downloading blob: {blob_name}")
        blob_data = blob_client.download_blob().readall()
        
        # Process the Excel file with blob name for group_id extraction
        logging.info(f"Processing Excel file: {blob_name}")
        tables = extractor.process_excel_content(blob_data, file_extension, os.path.basename(blob_name))
        
        if not tables:
            logging.info(f"No tables found in file: {blob_name}")
            return
        
        # Get output container name
        output_container_name = os.environ.get('OUTPUT_CONTAINER', 'output')
        
        # Ensure output container exists
        output_container_client = blob_service_client.get_container_client(output_container_name)
        if not output_container_client.exists():
            logging.info(f"Creating output container: {output_container_name}")
            output_container_client.create_container()
        
        # Upload each table as a CSV file
        base_name = os.path.splitext(os.path.basename(blob_name))[0]
        
        for sheet_name, csv_content in tables.items():
            # Create safe filename
            safe_sheet_name = "".join(c if c.isalnum() else "_" for c in sheet_name)
            output_blob_name = f"{base_name}_{safe_sheet_name}_table.csv"
            
            # Upload CSV to output container
            output_blob_client = output_container_client.get_blob_client(output_blob_name)
            output_blob_client.upload_blob(csv_content, overwrite=True)
            
            logging.info(f"Uploaded CSV: {output_blob_name} with {len(csv_content)} bytes")
        
        logging.info(f"Successfully processed {blob_name}. Extracted {len(tables)} tables.")
        
    except Exception as e:
        logging.error(f"Error processing event: {str(e)}")
        raise
