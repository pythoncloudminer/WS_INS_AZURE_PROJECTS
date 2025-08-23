import os
import pandas as pd
import numpy as np
from pyexcel import get_sheet
import csv
from typing import List, Tuple, Optional
import io

class ExcelTableExtractor:
    def __init__(self, value_threshold: float = 0.8, empty_threshold: float = 0.3):
        """
        Initialize the table extractor with thresholds
        
        Args:
            value_threshold: Minimum percentage of cells with values to be considered table header (default: 0.8)
            empty_threshold: Maximum percentage of cells with values to mark end of table (default: 0.3)
        """
        self.value_threshold = value_threshold
        self.empty_threshold = empty_threshold
    
    def extract_group_id(self, blob_name: str) -> str:
        """
        Extract group_id from blob name by splitting with '_' and taking the second value
        
        Args:
            blob_name: The name of the blob file
            
        Returns:
            Extracted group_id or empty string if not found
        """
        try:
            # Split the blob name by underscores
            parts = blob_name.split('_')
            # Take the second part (index 1) as group_id
            if len(parts) >= 2:
                return parts[1]
            else:
                print(f"Warning: Could not extract group_id from blob name: {blob_name}")
                return ""
        except Exception as e:
            print(f"Error extracting group_id: {e}")
            return ""
    
    def read_excel_sheet(self, file_content: bytes, sheet_name: str, file_extension: str) -> List[List]:
        """
        Read Excel sheet from bytes using pyexcel
        """
        try:
            # Create a file-like object from bytes
            file_obj = io.BytesIO(file_content)
            
            # Determine file type
            file_type = 'xlsx' if file_extension.lower() in ['.xlsx'] else 'xls'
            
            sheet = get_sheet(file_type=file_type, file_content=file_obj, sheet_name=sheet_name)
            return sheet.to_array()
        except Exception as e:
            print(f"Error reading sheet {sheet_name}: {e}")
            return []
    
    def find_table_boundaries(self, data: List[List]) -> Optional[Tuple[int, int, int, int]]:
        """
        Find table boundaries based on cell value percentages
        
        Returns:
            Tuple of (start_row, end_row, start_col, end_col) or None if no table found
        """
        if not data:
            return None
        
        rows = len(data)
        cols = max(len(row) for row in data) if data else 0
        
        if cols == 0:
            return None
        
        # Find the header row (first row with >80% cells having values)
        header_row = None
        for i in range(rows):
            row = data[i]
            if len(row) < cols:
                row = row + [None] * (cols - len(row))
            
            non_empty_count = sum(1 for cell in row if cell is not None and str(cell).strip() != '')
            if non_empty_count / cols >= self.value_threshold:
                header_row = i
                break
        
        if header_row is None:
            return None
        
        # Find the end of the table (first row after header with <30% cells having values)
        table_end = None
        for i in range(header_row + 1, rows):
            row = data[i]
            if len(row) < cols:
                row = row + [None] * (cols - len(row))
            
            non_empty_count = sum(1 for cell in row if cell is not None and str(cell).strip() != '')
            if non_empty_count / cols < self.empty_threshold:
                table_end = i - 1
                break
        
        # If no empty row found, table goes to the end
        if table_end is None:
            table_end = rows - 1
        
        # Find column boundaries
        start_col = 0
        end_col = cols - 1
        
        # Trim empty columns from left
        for col in range(cols):
            has_data = False
            for row in range(header_row, table_end + 1):
                if (row < len(data) and 
                    col < len(data[row]) and 
                    data[row][col] is not None and 
                    str(data[row][col]).strip() != ''):
                    has_data = True
                    break
            if has_data:
                start_col = col
                break
        
        # Trim empty columns from right
        for col in range(cols - 1, -1, -1):
            has_data = False
            for row in range(header_row, table_end + 1):
                if (row < len(data) and 
                    col < len(data[row]) and 
                    data[row][col] is not None and 
                    str(data[row][col]).strip() != ''):
                    has_data = True
                    break
            if has_data:
                end_col = col
                break
        
        return (header_row, table_end, start_col, end_col)
    
    def extract_table_data(self, data: List[List], boundaries: Tuple[int, int, int, int]) -> List[List]:
        """
        Extract table data based on boundaries
        """
        start_row, end_row, start_col, end_col = boundaries
        
        table_data = []
        for row_idx in range(start_row, end_row + 1):
            if row_idx < len(data):
                row = data[row_idx]
                # Ensure row has enough columns
                if len(row) <= end_col:
                    row = row + [None] * (end_col - len(row) + 1)
                table_row = row[start_col:end_col + 1]
                table_data.append(table_row)
        
        return table_data
    
    def add_group_column(self, table_data: List[List], group_id: str) -> List[List]:
        """
        Add a 'Group' column with the group_id to the table data
        
        Args:
            table_data: The extracted table data
            group_id: The group ID to add to each row
            
        Returns:
            Table data with added Group column
        """
        if not table_data:
            return table_data
        
        # Add Group column header if we have a header row
        if table_data and any(cell is not None and str(cell).strip() != '' for cell in table_data[0]):
            # Add "Group" to the header row
            table_data[0].append("Group")
        else:
            # If no header row, create one
            if table_data:
                table_data[0].append("Group")
            else:
                table_data.append(["Group"])
        
        # Add group_id to all data rows
        for i in range(1 if table_data and any(cell is not None and str(cell).strip() != '' for cell in table_data[0]) else 0, len(table_data)):
            table_data[i].append(group_id)
        
        return table_data
    
    def table_to_csv_bytes(self, data: List[List]) -> bytes:
        """
        Convert table data to CSV bytes
        """
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(data)
        return output.getvalue().encode('utf-8')
    
    def process_excel_content(self, file_content: bytes, file_extension: str, blob_name: str) -> dict:
        """
        Process Excel file content and extract tables from all sheets
        
        Args:
            file_content: Excel file content as bytes
            file_extension: File extension (.xls or .xlsx)
            blob_name: Name of the blob file for group_id extraction
            
        Returns:
            Dictionary with sheet names as keys and CSV content as bytes values
        """
        result = {}
        
        # Extract group_id from blob name
        group_id = self.extract_group_id(blob_name)
        print(f"Extracted group_id: '{group_id}' from blob: {blob_name}")
        
        # Get all sheet names
        try:
            if file_extension.lower() == '.xlsx':
                excel_file = pd.ExcelFile(io.BytesIO(file_content), engine='openpyxl')
            else:
                excel_file = pd.ExcelFile(io.BytesIO(file_content))
            
            sheet_names = excel_file.sheet_names
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            return result
        
        for sheet_name in sheet_names:
            print(f"Processing sheet: {sheet_name}")
            
            # Read sheet data
            sheet_data = self.read_excel_sheet(file_content, sheet_name, file_extension)
            
            if not sheet_data:
                print(f"No data found in sheet: {sheet_name}")
                continue
            
            # Find table boundaries
            boundaries = self.find_table_boundaries(sheet_data)
            
            if boundaries is None:
                print(f"No table found in sheet: {sheet_name}")
                continue
            
            # Extract table data
            table_data = self.extract_table_data(sheet_data, boundaries)
            
            if not table_data:
                print(f"No table data extracted from sheet: {sheet_name}")
                continue
            
            # Add Group column with group_id
            table_data_with_group = self.add_group_column(table_data, group_id)
            
            # Convert to CSV
            csv_bytes = self.table_to_csv_bytes(table_data_with_group)
            result[sheet_name] = csv_bytes
            
            print(f"Extracted table with {len(table_data_with_group)} rows and {len(table_data_with_group[0]) if table_data_with_group else 0} columns")
        
        return result
