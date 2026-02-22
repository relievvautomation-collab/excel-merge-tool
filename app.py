import os
import uuid
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import tempfile
import shutil
from datetime import datetime
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill
import traceback
import re
from collections import OrderedDict
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app, origins=["*"])

# Configuration
import os
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'xlsm', 'csv'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Store processed files temporarily
processed_files = {}

global_stats = {
    "totalSheetsMerged": 0,
    "todaySheetsMerged": 0,
    "lastResetDate": datetime.now().strftime("%Y-%m-%d")
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def smart_detect_header(df_raw, sheet_name, filename):
    """
    Smart header detection that analyzes patterns to find the correct header row
    """
    if df_raw.empty:
        return 0, []
    
    # Strategy 1: Look for row with maximum number of non-empty cells
    max_non_empty = 0
    header_candidate = 0
    
    for i in range(min(20, len(df_raw))):
        row = df_raw.iloc[i]
        non_empty = row.notna().sum()
        
        # Check if this row looks like a header (text values, not numbers)
        text_ratio = 0
        if non_empty > 0:
            text_cells = sum(1 for cell in row[:10] if isinstance(cell, str) and cell.strip())
            text_ratio = text_cells / min(10, non_empty)
        
        # Score based on non-empty count and text ratio
        score = non_empty + (text_ratio * 5)
        
        if score > max_non_empty:
            max_non_empty = score
            header_candidate = i
    
    # Strategy 2: Look for common header patterns in the candidate row
    header_row = df_raw.iloc[header_candidate]
    header_texts = [str(cell).strip().lower() for cell in header_row if pd.notna(cell)]
    
    common_header_keywords = [
        'employee', 'name', 'code', 'id', 'date', 'time', 'amount',
        'total', 'qty', 'quantity', 'price', 'cost', 'description',
        'debit', 'credit', 'balance', 'remarks', 'note'
    ]
    
    keyword_matches = sum(1 for text in header_texts 
                         if any(keyword in text for keyword in common_header_keywords))
    
    # If we have good keyword matches, use this row
    if keyword_matches >= 2:
        return header_candidate, header_row.tolist()
    
    # Strategy 3: Check if candidate row is followed by data rows (not all empty)
    if header_candidate + 1 < len(df_raw):
        next_row = df_raw.iloc[header_candidate + 1]
        next_row_non_empty = next_row.notna().sum()
        
        # If next row has data, current row is likely header
        if next_row_non_empty > 0:
            return header_candidate, header_row.tolist()
    
    # Fallback: Use first non-empty row as header
    for i in range(len(df_raw)):
        if df_raw.iloc[i].notna().sum() > 0:
            return i, df_raw.iloc[i].tolist()
    
    return 0, []

def preserve_special_characters(text):
    """Clean column names while preserving important special characters"""
    if pd.isna(text):
        return ''
    
    text = str(text).strip()
    
    # Preserve common special characters used in headers
    # Keep: alphanumeric, space, hyphen, underscore, slash, backslash, parentheses, brackets
    text = re.sub(r'[^\w\s\-_\/\\\(\)\[\]\.]', '', text)
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    return text

def read_excel_file_advanced(file_path, filename):
    """Advanced Excel file reader with better header detection and structure preservation"""
    all_sheets_data = []
    
    try:
        # Read all sheets
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        for sheet_name in sheet_names:
            try:
                # Read sheet without assuming header
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)
                
                if df_raw.empty:
                    continue
                
                # Remove completely empty rows and columns
                df_raw = df_raw.dropna(how='all', axis=0)
                df_raw = df_raw.dropna(how='all', axis=1)
                
                if df_raw.empty:
                    continue
                
                # Reset index after dropping rows
                df_raw = df_raw.reset_index(drop=True)
                
                # Smart header detection
                header_row_idx, header_values = smart_detect_header(df_raw, sheet_name, filename)
                
                # Create clean column names
                clean_columns = []
                for idx, col_value in enumerate(header_values):
                    if pd.isna(col_value) or str(col_value).strip() == '':
                        clean_columns.append(f"Column_{idx+1}")
                    else:
                        cleaned = preserve_special_characters(col_value)
                        if cleaned:
                            clean_columns.append(cleaned)
                        else:
                            clean_columns.append(f"Column_{idx+1}")
                
                # Ensure unique column names
                seen = {}
                for i, col in enumerate(clean_columns):
                    if col in seen:
                        count = seen[col] + 1
                        clean_columns[i] = f"{col}_{count}"
                        seen[col] = count
                    else:
                        seen[col] = 0
                
                # Extract data (starting from row after header)
                data_start = header_row_idx + 1
                
                if data_start < len(df_raw):
                    data_df = df_raw.iloc[data_start:].reset_index(drop=True)
                    
                    # Assign column names
                    if len(data_df.columns) > len(clean_columns):
                        # Add extra columns if needed
                        extra_cols = len(data_df.columns) - len(clean_columns)
                        clean_columns.extend([f"Column_{len(clean_columns)+i+1}" for i in range(extra_cols)])
                    
                    data_df.columns = clean_columns[:len(data_df.columns)]
                    
                    # Clean the dataframe
                    data_df = data_df.dropna(how='all', axis=0)
                    data_df = data_df.dropna(how='all', axis=1)
                    
                    # Fill NaN values with empty string
                    data_df = data_df.fillna('')
                    
                    # Add source columns
                    data_df.insert(0, 'Source_Sheet', sheet_name)
                    data_df.insert(0, 'Source_File', filename)
                    
                    # Get merged cells info
                    merged_cells = []
                    try:
                        wb = load_workbook(file_path, data_only=True, read_only=True)
                        ws = wb[sheet_name]
                        
                        for merged_range in ws.merged_cells.ranges:
                            merged_cells.append({
                                'min_row': merged_range.min_row,
                                'max_row': merged_range.max_row,
                                'min_col': merged_range.min_col,
                                'max_col': merged_range.max_col,
                                'value': ws.cell(row=merged_range.min_row, column=merged_range.min_col).value
                            })
                        wb.close()
                    except:
                        pass
                    
                    # Prepare sheet data
                    sheet_data = {
                        'sheet_name': sheet_name,
                        'filename': filename,
                        'tables': [{
                            'data': data_df,
                            'dataframe': data_df,
                            'header_data': [clean_columns],
                            'merged_cells': merged_cells,
                            'column_ids': clean_columns,
                            'filename': filename,
                            'sheet_name': sheet_name,
                            'original_header': header_values
                        }]
                    }
                    
                    all_sheets_data.append(sheet_data)
                    
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {str(e)[:100]}")
                continue
        
        return all_sheets_data
        
    except Exception as e:
        print(f"Error reading Excel file {filename}: {str(e)[:100]}")
        # Fallback to simple read
        return read_excel_file_simple(file_path, filename)

def read_excel_file_simple(file_path, filename):
    """Simple fallback Excel reader"""
    try:
        df = pd.read_excel(file_path, sheet_name=None, dtype=str)
        all_sheets_data = []
        
        for sheet_name, sheet_df in df.items():
            if sheet_df.empty:
                continue
            
            # Reset column names
            sheet_df = sheet_df.reset_index(drop=True)
            
            # Fill NaN values
            sheet_df = sheet_df.fillna('')
            
            # Add source columns
            sheet_df.insert(0, 'Source_Sheet', sheet_name)
            sheet_df.insert(0, 'Source_File', filename)
            
            # Get column names
            columns = list(sheet_df.columns)
            
            sheet_data = {
                'sheet_name': sheet_name,
                'filename': filename,
                'tables': [{
                    'data': sheet_df,
                    'dataframe': sheet_df,
                    'header_data': [columns],
                    'merged_cells': [],
                    'column_ids': columns,
                    'filename': filename,
                    'sheet_name': sheet_name,
                    'original_header': columns
                }]
            }
            
            all_sheets_data.append(sheet_data)
        
        return all_sheets_data
        
    except Exception as e:
        print(f"Simple read failed for {filename}: {str(e)[:100]}")
        return []

def read_csv_file_advanced(file_path, filename):
    """Advanced CSV reader with encoding detection"""
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16-le', 'utf-16-be']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, dtype=str, on_bad_lines='skip')
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue
        else:
            # If all encodings fail, try without specifying encoding
            try:
                df = pd.read_csv(file_path, dtype=str, on_bad_lines='skip')
            except:
                return []
        
        if df.empty:
            return []
        
        # Fill NaN values
        df = df.fillna('')
        
        # Clean column names
        clean_columns = []
        for col in df.columns:
            if pd.isna(col) or str(col).strip() == '':
                clean_columns.append(f"Column_{len(clean_columns)+1}")
            else:
                cleaned = preserve_special_characters(col)
                if cleaned:
                    clean_columns.append(cleaned)
                else:
                    clean_columns.append(f"Column_{len(clean_columns)+1}")
        
        df.columns = clean_columns
        
        # Add source columns
        df.insert(0, 'Source_Sheet', 'CSV_Sheet')
        df.insert(0, 'Source_File', filename)
        
        sheet_data = {
            'sheet_name': 'CSV_Sheet',
            'filename': filename,
            'tables': [{
                'data': df,
                'dataframe': df,
                'header_data': [clean_columns],
                'merged_cells': [],
                'column_ids': clean_columns,
                'filename': filename,
                'sheet_name': 'CSV_Sheet',
                'original_header': clean_columns
            }]
        }
        
        return [sheet_data]
        
    except Exception as e:
        print(f"Error reading CSV {filename}: {str(e)[:100]}")
        return []

def extract_file_data(file_path, filename):
    """Extract data from any supported file with improved accuracy"""
    try:
        if filename.lower().endswith('.csv'):
            return read_csv_file_advanced(file_path, filename)
        else:
            return read_excel_file_advanced(file_path, filename)
    except Exception as e:
        print(f"Error extracting data from {filename}: {str(e)[:100]}")
        traceback.print_exc()
        return []

def intelligent_column_matching(all_sheets_data):
    """
    Intelligently match columns across different sheets/files
    Returns a unified column order
    """
    if not all_sheets_data:
        return []
    
    # Collect all columns from all tables
    all_columns = OrderedDict()
    column_frequency = {}
    
    for sheet_data in all_sheets_data:
        for table_data in sheet_data.get('tables', []):
            df = table_data.get('dataframe')
            if df is not None:
                for col in df.columns:
                    # Clean column name for matching
                    clean_col = str(col).strip().lower()
                    
                    if clean_col in column_frequency:
                        column_frequency[clean_col] += 1
                    else:
                        column_frequency[clean_col] = 1
                    
                    # Store original column with its variations
                    if clean_col not in all_columns:
                        all_columns[clean_col] = col
                    elif column_frequency[clean_col] == column_frequency.get(all_columns[clean_col], 0):
                        # Prefer more descriptive column names
                        if len(str(col)) > len(str(all_columns[clean_col])):
                            all_columns[clean_col] = col
    
    # Create a unified column order
    # Put source columns first
    unified_columns = []
    
    # Add source file and sheet columns if they exist in any table
    source_cols = ['source_file', 'source_sheet']
    for source_col in source_cols:
        if source_col in all_columns:
            unified_columns.append(all_columns[source_col])
            del all_columns[source_col]
    
    # Add remaining columns sorted by frequency (most common first)
    sorted_cols = sorted(all_columns.items(), 
                        key=lambda x: column_frequency.get(x[0], 0), 
                        reverse=True)
    
    for clean_col, orig_col in sorted_cols:
        if clean_col not in [c.lower() for c in unified_columns]:
            unified_columns.append(orig_col)
    
    return unified_columns

def merge_dataframes_intelligently(all_dfs, unified_columns):
    """Merge dataframes intelligently using the unified column order"""
    if not all_dfs:
        return pd.DataFrame()
    
    merged_rows = []
    
    for df in all_dfs:
        # Create a mapping from df columns to unified columns
        column_map = {}
        for unified_col in unified_columns:
            # Try to find matching column (case-insensitive, ignoring special chars)
            unified_clean = str(unified_col).strip().lower()
            
            for df_col in df.columns:
                df_col_clean = str(df_col).strip().lower()
                if df_col_clean == unified_clean:
                    column_map[unified_col] = df_col
                    break
            
            # If no match found, this column will be empty for this dataframe
            if unified_col not in column_map:
                column_map[unified_col] = None
        
        # Process each row
        for _, row in df.iterrows():
            row_dict = {}
            
            for unified_col in unified_columns:
                df_col = column_map[unified_col]
                
                if df_col is not None and df_col in df.columns:
                    value = row[df_col]
                    
                    # Handle NaN/None
                    if pd.isna(value):
                        # Try to infer type from other rows
                        if df[df_col].dtype in ['int64', 'float64']:
                            row_dict[unified_col] = 0
                        else:
                            row_dict[unified_col] = ''
                    else:
                        # Try to convert to appropriate type
                        try:
                            # Try numeric conversion
                            if isinstance(value, str) and value.strip():
                                # Check if it looks like a number
                                if re.match(r'^-?\d+\.?\d*$', value.strip()):
                                    if '.' in value:
                                        row_dict[unified_col] = float(value)
                                    else:
                                        row_dict[unified_col] = int(value)
                                else:
                                    row_dict[unified_col] = value
                            else:
                                row_dict[unified_col] = value
                        except:
                            row_dict[unified_col] = value
                else:
                    # Column not in this dataframe, use appropriate default
                    if unified_col.lower() in ['source_file', 'source_sheet']:
                        row_dict[unified_col] = ''
                    else:
                        # Try to infer type from other dataframes
                        for other_df in all_dfs:
                            if unified_col in other_df.columns:
                                if other_df[unified_col].dtype in ['int64', 'float64']:
                                    row_dict[unified_col] = 0
                                    break
                        else:
                            row_dict[unified_col] = ''
            
            merged_rows.append(row_dict)
    
    # Create consolidated dataframe
    consolidated_df = pd.DataFrame(merged_rows, columns=unified_columns)
    
    # Ensure consistent data types
    for col in consolidated_df.columns:
        if col not in ['Source_File', 'Source_Sheet']:
            # Check if column contains numeric data
            numeric_count = 0
            total_count = 0
            
            for val in consolidated_df[col]:
                if pd.notna(val):
                    total_count += 1
                    if isinstance(val, (int, float, np.integer, np.floating)):
                        numeric_count += 1
                    elif isinstance(val, str) and re.match(r'^-?\d+\.?\d*$', val.strip()):
                        numeric_count += 1
            
            # Convert to numeric if majority are numbers
            if total_count > 0 and (numeric_count / total_count) > 0.5:
                try:
                    consolidated_df[col] = pd.to_numeric(consolidated_df[col], errors='coerce')
                    consolidated_df[col] = consolidated_df[col].fillna(0)
                except:
                    pass
    
    return consolidated_df

def merge_all_data(all_sheets_data):
    """Merge all data from all sheets with intelligent column matching"""
    if not all_sheets_data:
        return pd.DataFrame(), [], {}, {}
    
    all_dfs = []
    all_header_data = []
    all_merged_cells = []
    sheet_info = {}
    
    # Collect all dataframes and sheet info
    for sheet_data in all_sheets_data:
        if not sheet_data:
            continue
        
        sheet_name = sheet_data['sheet_name']
        filename = sheet_data['filename']
        
        for table_data in sheet_data.get('tables', []):
            df = table_data.get('dataframe')
            if df is not None and not df.empty:
                all_dfs.append(df)
                all_header_data.append(table_data.get('header_data', []))
                all_merged_cells.extend(table_data.get('merged_cells', []))
                
                # Store sheet info
                key = f"{filename} - {sheet_name}"
                if key not in sheet_info:
                    sheet_info[key] = {
                        'filename': filename,
                        'sheet_name': sheet_name,
                        'table_count': 0,
                        'row_count': len(df),
                        'column_count': len(df.columns)
                    }
                sheet_info[key]['table_count'] += 1
    
    if not all_dfs:
        return pd.DataFrame(), [], {}, {}
    
    try:
        # Intelligent column matching
        unified_columns = intelligent_column_matching(all_sheets_data)
        
        # Merge dataframes using unified columns
        consolidated_df = merge_dataframes_intelligently(all_dfs, unified_columns)
        
    except Exception as e:
        print(f"Error in intelligent merging: {str(e)[:200]}")
        traceback.print_exc()
        
        # Fallback to simple concatenation
        try:
            consolidated_df = pd.concat(all_dfs, ignore_index=True, sort=False)
            consolidated_df = consolidated_df.fillna('')
        except:
            consolidated_df = pd.DataFrame()
    
    return consolidated_df, all_header_data, all_merged_cells, sheet_info

def create_output_excel(df, output_path, header_data_list, merged_cells_list):
    """Create final Excel file with proper formatting"""
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Merged_Data"
        
        if df.empty:
            wb.save(output_path)
            return True
        
        # Start writing from row 1
        current_row = 1
        
        # Write headers
        for col_idx, col_name in enumerate(df.columns, 1):
            cell = ws.cell(row=current_row, column=col_idx, value=col_name)
            
            # Apply header styling
            cell.font = Font(bold=True, color="FFFFFF", size=11)
            cell.fill = PatternFill(start_color="1E3C72", end_color="1E3C72", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(
                left=Side(style='thin', color="000000"),
                right=Side(style='thin', color="000000"),
                top=Side(style='thin', color="000000"),
                bottom=Side(style='thin', color="000000")
            )
        
        current_row += 1
        
        # Write data rows
        for df_row_idx, (_, row) in enumerate(df.iterrows()):
            for col_idx, col_name in enumerate(df.columns, 1):
                value = row[col_name]
                cell = ws.cell(row=current_row, column=col_idx, value=value)
                
                # Apply data styling
                cell.border = Border(
                    left=Side(style='thin', color="E0E0E0"),
                    right=Side(style='thin', color="E0E0E0"),
                    top=Side(style='thin', color="E0E0E0"),
                    bottom=Side(style='thin', color="E0E0E0")
                )
                
                # Set alignment based on data type
                if isinstance(value, (int, float, np.integer, np.floating)):
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                    if isinstance(value, float):
                        cell.number_format = '#,##0.00'
                    else:
                        cell.number_format = '#,##0'
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center")
            
            current_row += 1
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            
            for cell in column[:500]:  # Check first 500 rows
                try:
                    if cell.value is not None:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
        # Freeze headers
        ws.freeze_panes = ws['A2']
        
        wb.save(output_path)
        return True
        
    except Exception as e:
        print(f"Error creating output Excel: {str(e)[:200]}")
        traceback.print_exc()
        return False

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/merge', methods=['POST'])
def merge_files():
    """API endpoint to merge uploaded files"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded', 'success': False}), 400
        
        files = request.files.getlist('files')
        if len(files) == 0:
            return jsonify({'error': 'No files selected', 'success': False}), 400
        
        session_id = str(uuid.uuid4())
        
        all_sheets_data = []
        total_tables = 0
        total_rows = 0
        total_columns = 0
        sheet_names_info = {}
        
        # Process each file
        for file in files:
            if not file or file.filename == '':
                continue
                
            if not allowed_file(file.filename):
                return jsonify({'error': f'File {file.filename} has invalid extension', 'success': False}), 400
            
            # Save file temporarily
            safe_filename = str(uuid.uuid4()) + "_" + file.filename
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
            file.save(temp_path)
            
            try:
                print(f"Processing: {file.filename}")
                
                # Extract data from file with improved accuracy
                sheets_data = extract_file_data(temp_path, file.filename)
                
                if sheets_data:
                    for sheet_data in sheets_data:
                        sheet_name = sheet_data['sheet_name']
                        key = f"{file.filename} - {sheet_name}"
                        
                        all_sheets_data.append(sheet_data)
                        
                        # Collect sheet info
                        if key not in sheet_names_info:
                            sheet_names_info[key] = {
                                'filename': file.filename,
                                'sheet_name': sheet_name,
                                'table_count': 0,
                                'row_count': 0,
                                'column_count': 0
                            }
                        
                        for table_data in sheet_data['tables']:
                            total_tables += 1
                            df = table_data.get('dataframe', pd.DataFrame())
                            
                            sheet_row_count = len(df)
                            sheet_column_count = len(df.columns)
                            
                            total_rows += sheet_row_count
                            total_columns = max(total_columns, sheet_column_count)
                            
                            sheet_names_info[key]['table_count'] += 1
                            sheet_names_info[key]['row_count'] += sheet_row_count
                            sheet_names_info[key]['column_count'] = max(
                                sheet_names_info[key]['column_count'], 
                                sheet_column_count
                            )
                    
                    print(f"  Found {len(sheets_data)} sheets with {total_tables} tables")
                else:
                    print(f"  No data found in {file.filename}")
                
            except Exception as e:
                print(f"Error processing {file.filename}: {str(e)[:200]}")
            finally:
                # Cleanup
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    except:
                        pass
        
        if not all_sheets_data:
            return jsonify({'error': 'No data found in uploaded files. Please ensure files contain data and are in supported formats (.xlsx, .xls, .xlsm, .csv).', 'success': False}), 400
        
        print(f"Total sheets found: {len(all_sheets_data)}")
        print(f"Total tables found: {total_tables}")
        
        # Merge all data with improved algorithm
        try:
            consolidated_df, header_data_list, merged_cells_list, sheet_info = merge_all_data(all_sheets_data)
            
            if consolidated_df.empty:
                return jsonify({'error': 'No data to merge after processing', 'success': False}), 400
            
            print(f"Merged data: {consolidated_df.shape[0]} rows, {consolidated_df.shape[1]} columns")
            
            # Prepare preview data
            preview_data = []
            
            # Add headers
            preview_data.append(consolidated_df.columns.tolist())
            
            # Add data rows (first 100)
            preview_rows = consolidated_df.head(100)
            for _, row in preview_rows.iterrows():
                row_list = []
                for val in row.tolist():
                    if isinstance(val, (np.integer, np.floating)):
                        row_list.append(float(val) if isinstance(val, np.floating) else int(val))
                    elif pd.isna(val):
                        row_list.append('')
                    else:
                        row_list.append(val)
                preview_data.append(row_list)
            
            # Save output file
            output_filename = f"merged_{session_id}.xlsx"
            output_path = os.path.join(UPLOAD_FOLDER, output_filename)
            
            success = create_output_excel(
                consolidated_df, output_path, header_data_list, merged_cells_list
            )
            
            if not success:
                return jsonify({'error': 'Failed to create output file', 'success': False}), 500
            
        except Exception as e:
            print(f"Error in merge process: {str(e)[:200]}")
            traceback.print_exc()
            return jsonify({'error': f'Error merging data: {str(e)[:200]}', 'success': False}), 500
        
        # Store file info
        processed_files[session_id] = {
            'filename': output_filename,
            'path': output_path,
            'created_at': datetime.now().isoformat(),
            'stats': {
                'tables': total_tables,
                'rows': len(consolidated_df),
                'columns': len(consolidated_df.columns),
                'files': len([f for f in files if f])
            },
            'sheet_info': sheet_names_info
        }
    
        today = datetime.now().strftime("%Y-%m-%d")
    
        if global_stats["lastResetDate"] != today:
            global_stats["todaySheetsMerged"] = 0
            global_stats["lastResetDate"] = today
            
        global_stats["totalSheetsMerged"] += total_tables
        global_stats["todaySheetsMerged"] += total_tables

        return jsonify({
            'success': True,
            'download_id': session_id,
        
        return jsonify({
            'success': True,
            'download_id': session_id,
            'data': {
                'consolidated': preview_data
            },
            'stats': {
                'tables': total_tables,
                'rows': len(consolidated_df),
                'columns': len(consolidated_df.columns),
                'files': len([f for f in files if f])
            },
            'sheet_info': sheet_names_info
        })
    
    except Exception as e:
        print(f"Error in merge endpoint: {str(e)[:200]}")
        traceback.print_exc()
        return jsonify({'error': str(e)[:200], 'success': False}), 500

@app.route('/download/<session_id>', methods=['GET'])
def download_file(session_id):
    """Download the merged Excel file"""
    try:
        if session_id not in processed_files:
            return jsonify({'error': 'File not found or expired', 'success': False}), 404
        
        file_info = processed_files[session_id]
        file_path = file_info['path']
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found', 'success': False}), 404
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=f"Merged_Excel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        print(f"Error in download endpoint: {str(e)[:200]}")
        traceback.print_exc()
        return jsonify({'error': str(e)[:200], 'success': False}), 500

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """Clean up old files"""
    try:
        cutoff_time = datetime.now().timestamp() - 3600
        cleaned_count = 0
        
        for session_id, file_info in list(processed_files.items()):
            file_path = file_info['path']
            if os.path.exists(file_path):
                file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                if file_age > 3600:
                    try:
                        os.remove(file_path)
                    except:
                        pass
                    del processed_files[session_id]
                    cleaned_count += 1
        
        for filename in os.listdir(UPLOAD_FOLDER):
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(file_path):
                file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                if file_age > 3600:
                    try:
                        os.remove(file_path)
                    except:
                        pass
        
        return jsonify({'success': True, 'cleaned': cleaned_count})
    
    except Exception as e:
        print(f"Error in cleanup: {str(e)[:200]}")
        traceback.print_exc()
        return jsonify({'error': str(e)[:200], 'success': False}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    return jsonify(global_stats)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'processed_files': len(processed_files)
    })

@app.route('/style.css')
def serve_css():
    return send_from_directory('.', 'style.css')

@app.route('/script.js')
def serve_js():
    return send_from_directory('.', 'script.js')

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
    print("=" * 70)
    print("EXCEL MULTI-FILE MERGE TOOL - ENHANCED VERSION")
    print("=" * 70)
    print(f"Upload folder: {os.path.abspath(UPLOAD_FOLDER)}")
    print("Server running on http://localhost:5000")
    print("Open http://localhost:5000 in your browser")
    print("=" * 70)
    
    # Clean up old files on startup
    try:
        for filename in os.listdir(UPLOAD_FOLDER):
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(file_path):
                file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                if file_age > 3600:
                    os.remove(file_path)
        print("Cleaned up old files")
    except:
        pass
    
    if __name__ == '__main__':
        import os
        port = int(os.environ.get("PORT", 10000))
        app.run(host='0.0.0.0', port=port)






