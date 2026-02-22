import os
import uuid
import json
import pandas as pd
from parallel_processor import read_excel_parallel
import numpy as np
from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
from datetime import datetime
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill
import traceback
import re
from collections import OrderedDict
import warnings
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

# Suppress warnings
warnings.filterwarnings('ignore')

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app, resources={r"/*": {"origins": "*"}})

# ---------- GLOBAL ERROR HANDLERS (return JSON instead of HTML) ----------
@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(e):
    return jsonify({'error': 'Total upload size exceeds 200MB limit. Please reduce file sizes.', 'success': False}), 413

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Resource not found', 'success': False}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({'error': 'Method not allowed', 'success': False}), 405

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Internal server error', 'success': False}), 500

@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    print("Unhandled Exception:", str(e))
    traceback.print_exc()
    return jsonify({'error': 'An unexpected error occurred', 'success': False}), 500

# ---------- CONFIGURATION ----------
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'xlsm', 'csv'}
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB total request size

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Store processed files temporarily
processed_files = {}

# Statistics file for persistence
STATS_FILE = os.path.join(os.getcwd(), 'stats.json')

def load_stats():
    """Load global statistics from JSON file."""
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print("Could not load stats, using defaults:", e)
    return {
        "totalSheetsMerged": 0,
        "todaySheetsMerged": 0,
        "lastResetDate": datetime.now().strftime("%Y-%m-%d")
    }

def save_stats(stats):
    """Save global statistics to JSON file."""
    try:
        with open(STATS_FILE, 'w') as f:
            json.dump(stats, f)
    except Exception as e:
        print("Could not save stats:", e)

# Global statistics (shared across all users, persisted)
global_stats = load_stats()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def smart_detect_header(df_raw, sheet_name, filename):
    """
    Smart header detection that analyzes patterns to find the correct header row
    """
    if df_raw.empty:
        return 0, []
    
    max_non_empty = 0
    header_candidate = 0
    
    for i in range(min(20, len(df_raw))):
        row = df_raw.iloc[i]
        non_empty = row.notna().sum()
        
        text_ratio = 0
        if non_empty > 0:
            text_cells = sum(1 for cell in row[:10] if isinstance(cell, str) and cell.strip())
            text_ratio = text_cells / min(10, non_empty)
        
        score = non_empty + (text_ratio * 5)
        if score > max_non_empty:
            max_non_empty = score
            header_candidate = i
    
    header_row = df_raw.iloc[header_candidate]
    header_texts = [str(cell).strip().lower() for cell in header_row if pd.notna(cell)]
    
    common_header_keywords = [
        'employee', 'name', 'code', 'id', 'date', 'time', 'amount',
        'total', 'qty', 'quantity', 'price', 'cost', 'description',
        'debit', 'credit', 'balance', 'remarks', 'note'
    ]
    
    keyword_matches = sum(1 for text in header_texts 
                         if any(keyword in text for keyword in common_header_keywords))
    
    if keyword_matches >= 2:
        return header_candidate, header_row.tolist()
    
    if header_candidate + 1 < len(df_raw):
        next_row = df_raw.iloc[header_candidate + 1]
        next_row_non_empty = next_row.notna().sum()
        if next_row_non_empty > 0:
            return header_candidate, header_row.tolist()
    
    for i in range(len(df_raw)):
        if df_raw.iloc[i].notna().sum() > 0:
            return i, df_raw.iloc[i].tolist()
    
    return 0, []

def preserve_special_characters(text):
    """Clean column names while preserving important special characters"""
    if pd.isna(text):
        return ''
    
    text = str(text).strip()
    text = re.sub(r'[^\w\s\-_\/\\\(\)\[\]\.]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def read_excel_file_advanced(file_path, filename):
    """Advanced Excel file reader with better header detection and structure preservation"""
    all_sheets_data = []
    
    try:
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        for sheet_name in sheet_names:
            try:
                df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)
                
                if df_raw.empty:
                    continue
                
                df_raw = df_raw.dropna(how='all', axis=0)
                df_raw = df_raw.dropna(how='all', axis=1)
                
                if df_raw.empty:
                    continue
                
                df_raw = df_raw.reset_index(drop=True)
                
                header_row_idx, header_values = smart_detect_header(df_raw, sheet_name, filename)
                
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
                
                seen = {}
                for i, col in enumerate(clean_columns):
                    if col in seen:
                        count = seen[col] + 1
                        clean_columns[i] = f"{col}_{count}"
                        seen[col] = count
                    else:
                        seen[col] = 0
                
                data_start = header_row_idx + 1
                
                if data_start < len(df_raw):
                    data_df = df_raw.iloc[data_start:].reset_index(drop=True)
                    
                    if len(data_df.columns) > len(clean_columns):
                        extra_cols = len(data_df.columns) - len(clean_columns)
                        clean_columns.extend([f"Column_{len(clean_columns)+i+1}" for i in range(extra_cols)])
                    
                    data_df.columns = clean_columns[:len(data_df.columns)]
                    
                    data_df = data_df.dropna(how='all', axis=0)
                    data_df = data_df.dropna(how='all', axis=1)
                    data_df = data_df.fillna('')
                    
                    data_df.insert(0, 'Source_Sheet', sheet_name)
                    data_df.insert(0, 'Source_File', filename)
                    
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
        return read_excel_file_simple(file_path, filename)

def read_excel_file_simple(file_path, filename):
    """Simple fallback Excel reader"""
    try:
        df = pd.read_excel(file_path, sheet_name=None, dtype=str)
        all_sheets_data = []
        
        for sheet_name, sheet_df in df.items():
            if sheet_df.empty:
                continue
            
            sheet_df = sheet_df.reset_index(drop=True)
            sheet_df = sheet_df.fillna('')
            sheet_df.insert(0, 'Source_Sheet', sheet_name)
            sheet_df.insert(0, 'Source_File', filename)
            
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
    """Advanced CSV reader with encoding detection and chunking for large files"""
    try:
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16-le', 'utf-16-be']
        
        df = None
        for encoding in encodings:
            try:
                # Use chunksize to handle large CSV files without loading everything into memory
                chunks = []
                for chunk in pd.read_csv(file_path, encoding=encoding, dtype=str, on_bad_lines='skip', chunksize=10000):
                    chunks.append(chunk)
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue
        else:
            try:
                chunks = []
                for chunk in pd.read_csv(file_path, dtype=str, on_bad_lines='skip', chunksize=10000):
                    chunks.append(chunk)
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
            except:
                return []
        
        if df is None or df.empty:
            return []
        
        df = df.fillna('')
        
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
    
    all_columns = OrderedDict()
    column_frequency = {}
    
    for sheet_data in all_sheets_data:
        for table_data in sheet_data.get('tables', []):
            df = table_data.get('dataframe')
            if df is not None:
                for col in df.columns:
                    clean_col = str(col).strip().lower()
                    
                    if clean_col in column_frequency:
                        column_frequency[clean_col] += 1
                    else:
                        column_frequency[clean_col] = 1
                    
                    if clean_col not in all_columns:
                        all_columns[clean_col] = col
                    elif column_frequency[clean_col] == column_frequency.get(all_columns[clean_col], 0):
                        if len(str(col)) > len(str(all_columns[clean_col])):
                            all_columns[clean_col] = col
    
    unified_columns = []
    
    source_cols = ['source_file', 'source_sheet']
    for source_col in source_cols:
        if source_col in all_columns:
            unified_columns.append(all_columns[source_col])
            del all_columns[source_col]
    
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
        column_map = {}
        for unified_col in unified_columns:
            unified_clean = str(unified_col).strip().lower()
            for df_col in df.columns:
                df_col_clean = str(df_col).strip().lower()
                if df_col_clean == unified_clean:
                    column_map[unified_col] = df_col
                    break
            if unified_col not in column_map:
                column_map[unified_col] = None
        
        for _, row in df.iterrows():
            row_dict = {}
            for unified_col in unified_columns:
                df_col = column_map[unified_col]
                
                if df_col is not None and df_col in df.columns:
                    value = row[df_col]
                    
                    if pd.isna(value):
                        if df[df_col].dtype in ['int64', 'float64']:
                            row_dict[unified_col] = 0
                        else:
                            row_dict[unified_col] = ''
                    else:
                        try:
                            if isinstance(value, str) and value.strip():
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
                    if unified_col.lower() in ['source_file', 'source_sheet']:
                        row_dict[unified_col] = ''
                    else:
                        for other_df in all_dfs:
                            if unified_col in other_df.columns:
                                if other_df[unified_col].dtype in ['int64', 'float64']:
                                    row_dict[unified_col] = 0
                                    break
                        else:
                            row_dict[unified_col] = ''
            
            merged_rows.append(row_dict)
    
    consolidated_df = pd.DataFrame(merged_rows, columns=unified_columns)
    
    for col in consolidated_df.columns:
        if col not in ['Source_File', 'Source_Sheet']:
            numeric_count = 0
            total_count = 0
            for val in consolidated_df[col]:
                if pd.notna(val):
                    total_count += 1
                    if isinstance(val, (int, float, np.integer, np.floating)):
                        numeric_count += 1
                    elif isinstance(val, str) and re.match(r'^-?\d+\.?\d*$', val.strip()):
                        numeric_count += 1
            
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
        unified_columns = intelligent_column_matching(all_sheets_data)
        consolidated_df = merge_dataframes_intelligently(all_dfs, unified_columns)
    except Exception as e:
        print(f"Error in intelligent merging: {str(e)[:200]}")
        traceback.print_exc()
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
        
        current_row = 1
        
        for col_idx, col_name in enumerate(df.columns, 1):
            cell = ws.cell(row=current_row, column=col_idx, value=col_name)
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
        
        for _, row in df.iterrows():
            for col_idx, col_name in enumerate(df.columns, 1):
                value = row[col_name]
                cell = ws.cell(row=current_row, column=col_idx, value=value)
                cell.border = Border(
                    left=Side(style='thin', color="E0E0E0"),
                    right=Side(style='thin', color="E0E0E0"),
                    top=Side(style='thin', color="E0E0E0"),
                    bottom=Side(style='thin', color="E0E0E0")
                )
                if isinstance(value, (int, float, np.integer, np.floating)):
                    cell.alignment = Alignment(horizontal="right", vertical="center")
                    if isinstance(value, float):
                        cell.number_format = '#,##0.00'
                    else:
                        cell.number_format = '#,##0'
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center")
            current_row += 1
        
        for column in ws.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            for cell in column[:500]:
                try:
                    if cell.value is not None:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
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
        
        # Save all uploaded files first
        temp_paths = []
        for file in files:
            if not file or file.filename == '':
                continue
            if not allowed_file(file.filename):
                return jsonify({'error': f'File {file.filename} has invalid extension', 'success': False}), 400
            
            safe_filename = str(uuid.uuid4()) + "_" + file.filename
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
            file.save(temp_path)
            temp_paths.append(temp_path)
        
        if not temp_paths:
            return jsonify({'error': 'No valid files uploaded', 'success': False}), 400
        
        try:
            print(f"Processing {len(temp_paths)} files in parallel...")
            # Process all files in parallel using the imported function
            all_sheets_data = read_excel_parallel(temp_paths)
            
            if not all_sheets_data:
                return jsonify({'error': 'No data found in uploaded files. Please ensure files contain data and are in supported formats (.xlsx, .xls, .xlsm, .csv).', 'success': False}), 400
            
            # Compute totals and sheet info
            total_tables = 0
            total_rows = 0
            total_columns = 0
            sheet_names_info = {}
            
            for sheet_data in all_sheets_data:
                sheet_name = sheet_data['sheet_name']
                filename = sheet_data['filename']
                key = f"{filename} - {sheet_name}"
                
                if key not in sheet_names_info:
                    sheet_names_info[key] = {
                        'filename': filename,
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
            
            print(f"Total sheets found: {len(all_sheets_data)}")
            print(f"Total tables found: {total_tables}")
            
            # Merge all data
            try:
                consolidated_df, header_data_list, merged_cells_list, sheet_info = merge_all_data(all_sheets_data)
                
                if consolidated_df.empty:
                    return jsonify({'error': 'No data to merge after processing', 'success': False}), 400
                
                print(f"Merged data: {consolidated_df.shape[0]} rows, {consolidated_df.shape[1]} columns")
                
                # Prepare preview data
                preview_data = []
                preview_data.append(consolidated_df.columns.tolist())
                
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
                    'files': len(files)
                },
                'sheet_info': sheet_names_info
            }

            # Update global statistics (persisted)
            global global_stats
            today = datetime.now().strftime("%Y-%m-%d")
            if global_stats["lastResetDate"] != today:
                global_stats["todaySheetsMerged"] = 0
                global_stats["lastResetDate"] = today

            global_stats["totalSheetsMerged"] += total_tables
            global_stats["todaySheetsMerged"] += total_tables
            save_stats(global_stats)

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
                    'files': len(files)
                },
                'sheet_info': sheet_names_info
            })
        
        except Exception as e:
            print(f"Error in parallel processing: {str(e)[:200]}")
            traceback.print_exc()
            return jsonify({'error': f'Error processing files: {str(e)[:200]}', 'success': False}), 500
        
        finally:
            # Clean up uploaded files
            for temp_path in temp_paths:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except:
                    pass
    
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
    print("Server running on http://0.0.0.0:10000")
    print("Open your browser")
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
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
