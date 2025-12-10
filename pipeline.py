#!/usr/bin/env python
"""
Automated Pipeline: Download PDFs from Google Drive -> Extract JSON via Gemini -> Aggregate to Excel

Usage:
    python pipeline.py --drive-folder <FOLDER_ID_OR_URL> --output combined_output.xlsx

Example:
    python pipeline.py --drive-folder <FOLDER_ID> --output combined_output.xlsx
    python pipeline.py --drive-folder "<URL>" --output combined_output.xlsx

Requirements:
    - Google Drive folder must be publicly shared (Anyone with the link → Viewer)
    - GENAI_API_KEY must be set in .env or environment variable
    - Dependencies: gdown, google-generativeai, openpyxl, python-dotenv
"""

import os
import re
import sys
import json
import time
import shutil
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Any, Union, List
from datetime import datetime

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

import gdown
import google.generativeai as genai
from openpyxl import Workbook
import pandas as pd

import io
import pickle
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Union, List
import hashlib

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
REMOTE_PDFS_DIR = SCRIPT_DIR / 'remote_pdfs'
JSON_OUTPUTS_DIR = SCRIPT_DIR / 'json_outputs'
SAMPLE_PDF = SCRIPT_DIR / 'sample.pdf'
PROCESSED_FILES_DB = Path("processed_files.json")


PROMPT = """
Extract all information from this payment voucher document and return it in the following JSON structure. Ensure all fields are accurately extracted:

{
  "institute_details": {
    "institute_name": "[Extract full institute name]",
    "document_type": "[Extract document type, e.g., 'Payment Voucher']",
    "payment_mode_options": ["[List all payment mode options if visible]"]
    "payment_mode": ["One from the options that is ticked 'Main' or 'Project', 'Main + Project' or 'Imprest'"]
  },
  "voucher_details": {
    "date": "[Extract voucher date in DD/MM/YYYY format]",
    "department_section": "[Extract department or section name]",
    "general_info": {
      "unique_reference_number": "[Extract unique reference/voucher number]",
      "invoice_no": "[Extract invoice number]",
      "invoice_date": "[Extract invoice date in DD/MM/YYYY format]",
      "name_of_the_supplier": "[Extract supplier name exactly as shown]",
      "payment_to_be_made_in_the_name_of": "[Extract payee name]",
      "purchase_type": "[Extract: 'Import' or 'Indigenous']"
    }
  },
  "bill_details": {
    "items_claimed": [
      {
        "sr_no": [Item serial number as integer],
        "type_of_stock": "[Extract: 'Asset', 'Consumables (purchased separately)', 'Service', etc.]",
        "subcategory_of_stock": "[Extract subcategory: 'Chemicals', 'Equipment', etc.]",
        "item_description": "[Extract complete item description]",
        "net_amount_inr": [Extract net amount as decimal number],
        "remarks": "[Extract any remarks or notes for this item]"
      }
      // Repeat for each item in the bill
    ]
  },
  "amount_summary": {
    "total_amount_inr": [Extract total amount as decimal],
    "advance_taken_inr": [Extract advance amount as decimal, use 0.0 if not applicable],
    "penalty_deducted_inr": [Extract penalty amount as decimal, use 0.0 if not applicable],
    "net_amount_payable_figure_inr": [Extract net payable amount as decimal],
    "net_amount_payable_words": "[Extract amount in words in UPPERCASE]"
  },
  "project_fund_details": {
    "project_no": "[Extract project number/code]",
    "project_title": "[Extract project title]",
    "balance_in_project": [Extract balance as decimal or null if not shown],
    "overhead_deducted": [Extract overhead as decimal or null if not shown],
    "source_of_payment_options": ["[List all source options if visible]"],
    "source_of_payment": ["One from the options that is ticked 'Institute' or 'Department', 'CPDA' or 'IP' or 'RIG' or 'Project' or 'PDA' or 'DPA' or 'Endowment' or 'Not Applicable'"]
    "head_of_expense_options": ["[List all expense head options if visible]"], 
    "head_of_expense": ["One from the options that is ticked 'Equipment' or 'Consumable', 'Contingency' or 'Others' or 'Travel' or 'Service heads' or 'Not Applicable'"]
    
  }
  
}

IMPORTANT EXTRACTION RULES:
1. Extract ALL items from the bill_details section - create a separate object for each item
2. Use null for fields that are not present or not filled
3. Convert all numeric amounts to decimal numbers (not strings)
4. Return ONLY valid JSON without any additional text or explanation
"""

EXCEL_COLUMNS = [
    'Date',
    'Source PDF',
    'Unique Reference Number',
    'Invoice No.',
    'Invoice Date',
    'Name of the Supplier',
    'Payment to be made in the name of',
    'Purchase Type (Import/Indigenous)',
    'Type of Stock (Asset/Cons./Service)',
    'Subcategory of the stock',
    'Description of the Item (Item Name)',
    'Net Amount',
    'Remarks',
    'Total Amount (INR)',
    'Advance taken (if any) in INR',
    'Less: Penalty Deducted in INR',
    'Net Amount Payable (figure) INR',
    'Net Amount Payable (words) INR',
    'Project No',
    'Project Title',
    'Balance in Project',
    'Overhead Deducted',
    'Source of payment',
    'Head of expense'
]

# ============================================================================
# Helper Functions
# ============================================================================
#-------------------------------------------------------

def get_file_hash(filepath: Path) -> str:
    """Generate unique hash for file content."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def load_processed_files() -> Dict[str, dict]:
    """Load the database of processed files."""
    if not PROCESSED_FILES_DB.exists():
        return {}
    
    try:
        with open(PROCESSED_FILES_DB, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load processed files database: {e}")
        return {}

def save_processed_files(processed: Dict[str, dict]):
    """Save the database of processed files."""
    try:
        with open(PROCESSED_FILES_DB, 'w') as f:
            json.dump(processed, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save processed files database: {e}")


def mark_file_as_processed(filepath: Path, file_hash: str):
    """Mark a file as processed."""
    from datetime import datetime
    
    processed = load_processed_files()
    processed[file_hash] = {
        'filename': filepath.name,
        'processed_date': datetime.now().isoformat(),
        'file_path': str(filepath)
    }
    save_processed_files(processed)

def is_file_processed(file_hash: str) -> bool:
    """Check if a file has been processed."""
    processed = load_processed_files()
    return file_hash in processed



def get_unprocessed_files(pdf_files: List[Path]) -> List[Path]:
    """
    Filter list of PDF files to only include unprocessed ones.
    
    Args:
        pdf_files: List of PDF file paths
        
    Returns:
        List of unprocessed PDF file paths
    """
    unprocessed = []
    processed_db = load_processed_files()
    
    print(f"\nChecking {len(pdf_files)} file(s) against processed database...")
    
    for pdf_file in pdf_files:
        file_hash = get_file_hash(pdf_file)
        
        if file_hash in processed_db:
            prev_processed = processed_db[file_hash]
            print(f"  ⊗ SKIP: {pdf_file.name} (already processed on {prev_processed['processed_date'][:10]})")
        else:
            print(f"  ✓ NEW:  {pdf_file.name}")
            unprocessed.append(pdf_file)
    
    print(f"\nSummary:")
    print(f"  Total files: {len(pdf_files)}")
    print(f"  Already processed: {len(pdf_files) - len(unprocessed)}")
    print(f"  New to process: {len(unprocessed)}")
    
    return unprocessed


def list_processed_files():
    """Display all processed files."""
    processed = load_processed_files()
    
    if not processed:
        print("\nNo files have been processed yet.")
        return
    
    print(f"\n{'='*70}")
    print(f"PROCESSED FILES DATABASE ({len(processed)} files)")
    print(f"{'='*70}\n")
    
    for file_hash, info in sorted(processed.items(), key=lambda x: x[1]['processed_date'], reverse=True):
        print(f"📄 {info['filename']}")
        print(f"   Processed: {info['processed_date'][:19]}")
        print(f"   Hash: {file_hash[:16]}...")
        print()


def reset_processed_files():
    """Clear the processed files database."""
    if PROCESSED_FILES_DB.exists():
        PROCESSED_FILES_DB.unlink()
        print("✓ Processed files database cleared.")
    else:
        print("No database to clear.")


def remove_file_from_processed(filename: str):
    """Remove a specific file from processed database by filename."""
    processed = load_processed_files()
    
    found = []
    for file_hash, info in processed.items():
        if info['filename'] == filename:
            found.append(file_hash)
    
    if not found:
        print(f"File '{filename}' not found in processed database.")
        return
    
    for file_hash in found:
        del processed[file_hash]
        print(f"✓ Removed: {filename}")
    
    save_processed_files(processed)


#--------------------------------------------------------

def extract_folder_id(folder_input: str) -> str:
    """Extract folder ID from URL or return as-is if already an ID."""
    match = re.search(r'folders/([A-Za-z0-9_-]+)', folder_input)
    if match:
        return match.group(1)
    # Assume it's already a folder ID
    return folder_input.strip()

def download_pdfs_from_drive(folder_id: str, output_dir: Path) -> list:
    """
    Download ALL files from Drive folder (no date filtering).
    This replaces the download_pdfs_from_drive function.
    """
    
    output_dir.mkdir(exist_ok=True)

    # Existing PDFs in remote_pdfs
    # existing_names = {p.name for p in output_dir.glob("*.pdf")}
    
    # # Clean previous downloads
    # for f in output_dir.glob('*'):
    #     if f.is_file():
    #         f.unlink()
    
    url = f'https://drive.google.com/drive/folders/{folder_id}'
    print(f'\n[1/4] Downloading ALL PDFs from Google Drive folder: {url}')
    
    try:
        gdown.download_folder(url, output=str(output_dir), quiet=False)
    except Exception as e:
        print(f'Error downloading folder: {e}')
        return []
    
    pdf_files = sorted(output_dir.glob('*.pdf'))
    print(f'Downloaded {len(pdf_files)} PDF(s)')
    
    # Filter to only unprocessed files
    unprocessed_files = get_unprocessed_files(pdf_files)
    
    return unprocessed_files

#------------------------------------------------

def extract_folder_id(folder_input: str) -> str:
    """Extract folder ID from URL or return as-is if already an ID."""
    match = re.search(r'folders/([A-Za-z0-9_-]+)', folder_input)
    if match:
        return match.group(1)
    # Assume it's already a folder ID
    return folder_input.strip()


def extract_text_from_response(response):
    """Robustly extract text from Gemini API response."""
    json_output = None
    try:
        json_output = getattr(response, 'text', None)
    except Exception:
        json_output = None

    if not json_output:
        try:
            res = getattr(response, 'result', response)
            candidates = getattr(res, 'candidates', None)
            if candidates and len(candidates) > 0:
                candidate = candidates[0]
                content = getattr(candidate, 'content', None)
                if content:
                    parts = getattr(content, 'parts', None)
                    if parts and len(parts) > 0:
                        json_output = getattr(parts[0], 'text', None)
        except Exception:
            json_output = None

    if not json_output:
        try:
            json_output = str(response)
        except Exception:
            json_output = ''

    return json_output


def wait_for_file_processing(file_ref, poll_interval=2, timeout=120):
    """Wait for uploaded file to finish processing."""
    start = time.time()
    while getattr(file_ref.state, 'name', '') == 'PROCESSING':
        if time.time() - start > timeout:
            raise TimeoutError(f'Timeout waiting for file {file_ref.name}')
        time.sleep(poll_interval)
        file_ref = genai.get_file(file_ref.name)
    return file_ref


def process_pdf_with_gemini(pdf_path: Path, model) -> dict:
    """Upload PDF to Gemini, extract content, and return parsed JSON or raw text."""
    print(f'  Uploading: {pdf_path.name}')
    pdf_file = genai.upload_file(str(pdf_path))
    print(f'  Uploaded: {pdf_file.name}')
    
    pdf_file = wait_for_file_processing(pdf_file)
    if pdf_file.state.name == 'FAILED':
        raise ValueError(f'File processing failed for {pdf_path.name}')
    
    print(f'  Extracting content...')
    response = model.generate_content([PROMPT, pdf_file])
    text = extract_text_from_response(response)
    
    # Try to parse JSON
    parsed = None
    try:
        parsed = json.loads(text)
    except Exception:
        # Try stripping markdown code fences
        t = text.strip()
        if t.startswith('```'):
            t = t.replace('```json', '').replace('```', '').strip()
        try:
            parsed = json.loads(t)
        except Exception:
            parsed = text  # Return raw text if parsing fails
    
    return parsed


def process_all_pdfs(pdf_files: list, output_dir: Path) -> list:
    """Process all PDFs and save individual JSON outputs."""
    output_dir.mkdir(exist_ok=True)
    
    # Clean previous JSON outputs
    for f in output_dir.glob('*.json'):
        f.unlink()
    
    api_key = os.environ.get('GENAI_API_KEY')
    if not api_key:
        print('ERROR: GENAI_API_KEY not set in environment or .env')
        sys.exit(1)
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-flash-latest')
    
    print(f'\n[2/4] Processing {len(pdf_files)} PDF(s) with Gemini...')
    
    results = []
    for pdf_path in pdf_files:
        basename = pdf_path.stem
        json_path = output_dir / f'{basename}.json'
        
        try:
            parsed = process_pdf_with_gemini(pdf_path, model)
            
            # Save JSON output
            with open(json_path, 'w', encoding='utf-8') as f:
                if isinstance(parsed, dict):
                    json.dump(parsed, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(parsed))
            
            print(f'  ✓ Saved: {json_path.name}')
            results.append((basename, parsed, None))
            file_hash = get_file_hash(pdf_path)
            mark_file_as_processed(pdf_path, file_hash)
            
        except Exception as e:
            print(f'  ✗ Error processing {pdf_path.name}: {e}')
            results.append((basename, None, str(e)))
    
    return results


def build_rows_from_parsed(parsed, source_pdf: str) -> list:
    """Convert parsed JSON to Excel rows."""
    rows = []
    
    if not parsed:
        return rows
    
    # Handle raw string
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except Exception:
            preview = parsed.replace('\n', ' ')[:300]
            rows.append([source_pdf] + [''] * 10 + [f'PARSE_FAILED: {preview}'] + [''] * 11)
            return rows
    
    if not isinstance(parsed, dict):
        return rows
    
    # Navigate the JSON structure - handle different naming conventions
    pv = (
        parsed.get('payment_voucher') or
        parsed.get('PaymentVoucher') or
        parsed
    )
    
    # Try different key patterns for general details
    gen = (
        pv.get('general_details', {}) or
        pv.get('voucher_metadata', {}) or
        pv.get('reference_details', {}) or
        pv.get('general_information', {}) or
        pv.get('VoucherDetails', {}) or
        pv.get('HeaderInfo', {})
    )
    
    # Try different key patterns for items
    items = (
        pv.get('details_of_bills_claimed', []) or
        pv.get('bills_claimed_details', []) or
        pv.get('bill_details', {}).get('items_claimed', []) or
        pv.get('claimed_items', {}).get('items', []) or
        pv.get('details_of_bills', {}).get('items', []) or
        pv.get('items', []) or
        pv.get('details', []) or
        pv.get('ItemDetails', {}).get('BillsClaimed', []) or
        pv.get('ItemDetails', [])
    )
    # Ensure items is a list
    if isinstance(items, dict):
        items = items.get('items', []) or items.get('BillsClaimed', []) or items.get('items_claimed', []) or []
    
    # Try different key patterns for amount summary
    amount = (
        pv.get('amount_summary', {}) or
        pv.get('financial_summary', {}) or
        pv.get('amount_details', {}) or
        pv.get('details_of_bills', {}).get('amount_summary', {}) or
        pv.get('bill_details', {}).get('amount_summary', {}) or
        pv.get('FinancialSummary', {})
    )
    
    # Try different key patterns for project details
    proj = (
        pv.get('project_fund_details', {}) or
        pv.get('project_details', {}) or
        pv.get('ProjectFundDetails', {})
    )
    
    # Extract project_no and project_title from nested structure if needed
    project_no = proj.get('project_no', '') or proj.get('ProjectNo', '')
    project_title = proj.get('project_title', '') or proj.get('ProjectTitle', '')
    if not project_no and 'items' in proj:
        for item in proj.get('items', []):
            if item.get('contents') == 'Project No':
                project_no = item.get('details', '')
            elif item.get('contents') == 'Project Title':
                project_title = item.get('details', '')
    
    # Extract source_of_payment and head_of_expense
    source_of_payment = proj.get('source_of_payment', '') or proj.get('SourceOfPayment', '')
    head_of_expense = proj.get('head_of_expense', '') or proj.get('HeadOfExpense', '')
    
    # Handle dict-based source/head (where keys are boolean)
    admin = pv.get('administrative_approvals', {}) or pv.get('AccountingClassification', {})
    categorization = pv.get('categorization_of_expense', {})
    
    if isinstance(admin.get('source_of_payment'), dict):
        source_of_payment = ', '.join([k for k, v in admin['source_of_payment'].items() if v])
    if isinstance(admin.get('head_of_expense'), dict):
        head_of_expense = ', '.join([k for k, v in admin['head_of_expense'].items() if v])
    
    # Handle categorization structure with 'selected' field
    if categorization:
        if isinstance(categorization.get('source_of_payment'), dict):
            source_of_payment = categorization['source_of_payment'].get('selected', '')
        if isinstance(categorization.get('head_of_expense'), dict):
            head_of_expense = categorization['head_of_expense'].get('selected', '')
    
    if not items:
        # Single row for documents without itemized bills
        row = [
            source_pdf,
            gen.get('unique_reference_number', ''),
            gen.get('invoice_no', ''),
            gen.get('invoice_date', ''),
            gen.get('name_of_the_supplier', '') or gen.get('supplier_name', ''),
            gen.get('payment_to_be_made_in_the_name_of', '') or gen.get('payment_to_name', ''),
            gen.get('purchase_type', ''),
            '', '',
            '',
            amount.get('net_amount_payable_in_figure_inr', '') or amount.get('net_amount_payable_figure_inr', ''),
            '',
            amount.get('total_amount_in_inr', ''),
            amount.get('advance_taken_in_inr', ''),
            amount.get('penalty_deducted_in_inr', ''),
            amount.get('net_amount_payable_in_figure_inr', '') or amount.get('net_amount_payable_figure_inr', ''),
            amount.get('net_amount_payable_in_words_inr', '') or amount.get('net_amount_payable_words_inr', ''),
            project_no,
            project_title,
            proj.get('balance_in_project', ''),
            proj.get('overhead_deducted', ''),
            source_of_payment,
            head_of_expense
        ]
        rows.append(row)
        return rows
    
    # One row per item
    for it in items:
        row = [
            source_pdf,
            gen.get('unique_reference_number', '') or gen.get('UniqueReferenceNumber', ''),
            gen.get('invoice_no', '') or gen.get('InvoiceNo', ''),
            gen.get('invoice_date', '') or gen.get('InvoiceDate', ''),
            gen.get('name_of_the_supplier', '') or gen.get('supplier_name', '') or gen.get('SupplierName', ''),
            gen.get('payment_to_be_made_in_the_name_of', '') or gen.get('payment_to_name', '') or gen.get('payment_to_be_made_in_name_of', '') or gen.get('PaymentInNameOf', ''),
            gen.get('purchase_type', '') or gen.get('PurchaseType', ''),
            it.get('type_of_stock', '') or it.get('TypeOfStock', '') or it.get('TypeofStock_Asset_ConsService', ''),
            it.get('subcategory_of_the_stock', '') or it.get('subcategory_of_stock', '') or it.get('SubcategoryOfStock', ''),
            it.get('item_name', '') or it.get('description_item_name', '') or it.get('description', '') or it.get('Description', '') or it.get('ItemName', ''),
            it.get('net_amount', '') or it.get('net_amount_inr', '') or it.get('NetAmount', ''),
            it.get('remarks', '') or it.get('Remarks', ''),
            amount.get('total_amount_in_inr', '') or amount.get('total_amount_inr', '') or amount.get('total_amount', '') or amount.get('TotalAmountINR', ''),
            amount.get('advance_taken_in_inr', '') or amount.get('advance_taken_inr', '') or amount.get('advance_taken', '') or amount.get('AdvanceTakenINR', ''),
            amount.get('penalty_deducted_in_inr', '') or amount.get('penalty_deducted_inr', '') or amount.get('penalty_deducted', '') or amount.get('PenaltyDeductedINR', ''),
            amount.get('net_amount_payable_in_figure_inr', '') or amount.get('net_amount_payable_figure_inr', '') or amount.get('net_amount_payable', '') or amount.get('NetAmountPayableFigureINR', ''),
            amount.get('net_amount_payable_in_words_inr', '') or amount.get('net_amount_payable_words_inr', '') or amount.get('net_amount_payable_words', '') or amount.get('NetAmountPayableWordsINR', ''),
            project_no,
            project_title,
            proj.get('balance_in_project', '') or proj.get('BalanceInProject', ''),
            proj.get('overhead_deducted', '') or proj.get('OverheadDeducted', ''),
            source_of_payment,
            head_of_expense
        ]
        rows.append(row)
    
    return rows


def process_single_json(json_data: Dict[str, Any], source_pdf: str = "") -> List[Dict[str, Any]]:
    """
    Process a single JSON voucher and return rows for Excel.
    
    Args:
        json_data: Dictionary containing payment voucher data
        source_pdf: Name of source PDF file
        
    Returns:
        List of row dictionaries
    """
    # Extract common fields from JSON
    voucher = json_data.get('voucher_details', {})
    general = voucher.get('general_info', {})
    bill = json_data.get('bill_details', {})
    amount = json_data.get('amount_summary', {})
    project = json_data.get('project_fund_details', {})
    
    # Get items list
    items = bill.get('items_claimed', [])
    
    # Get current date for Date field
    current_date = datetime.now().strftime('%d/%m/%Y')
    
    # Create rows for Excel - one row per item
    rows = []
    
    for item in items:
        row = {
            'Date': current_date,
            'Source PDF': source_pdf,
            'Unique Reference Number': general.get('unique_reference_number', ''),
            'Invoice No.': general.get('invoice_no', ''),
            'Invoice Date': general.get('invoice_date', ''),
            'Name of the Supplier': general.get('name_of_the_supplier', ''),
            'Payment to be made in the name of': general.get('payment_to_be_made_in_the_name_of', ''),
            'Purchase Type (Import/Indigenous)': general.get('purchase_type', ''),
            'Type of Stock (Asset/Cons./Service)': item.get('type_of_stock', ''),
            'Subcategory of the stock': item.get('subcategory_of_stock', ''),
            'Description of the Item (Item Name)': item.get('item_description', ''),
            'Net Amount': item.get('net_amount_inr', 0),
            'Remarks': item.get('remarks', ''),
            'Total Amount (INR)': amount.get('total_amount_inr', 0),
            'Advance taken (if any) in INR': amount.get('advance_taken_inr', 0),
            'Less: Penalty Deducted in INR': amount.get('penalty_deducted_inr', 0),
            'Net Amount Payable (figure) INR': amount.get('net_amount_payable_figure_inr', 0),
            'Net Amount Payable (words) INR': amount.get('net_amount_payable_words', ''),
            'Project No': project.get('project_no', ''),
            'Project Title': project.get('project_title', ''),
            'Balance in Project': project.get('balance_in_project', ''),
            'Overhead Deducted': project.get('overhead_deducted', ''),
            'Source of payment': project.get('source_of_payment', ''),
            'Head of expense': project.get('head_of_expense', '')
        }
        rows.append(row)
    
    return rows


def aggregate_to_excel(json_input: Union[str, Path, Dict[str, Any]], output_file: str, source_pdf: str = "", append_mode: bool = False) -> None:
    """
    Convert payment voucher JSON data to Excel format.
    Can handle single JSON dict, single JSON file, or directory of JSON files.
    
    Args:
        json_input: Can be:
            - Dictionary: Single JSON data
            - String/Path to file: Single JSON file
            - String/Path to directory: Directory containing JSON files
        output_file: Path to output Excel file
        source_pdf: Name of source PDF file (only used for single dict input)
        append_mode: If True, append to existing Excel file; if False, create new file
    """
    all_rows = []
    
    # Handle different input types
    if isinstance(json_input, dict):
        # Direct JSON dictionary
        all_rows.extend(process_single_json(json_input, source_pdf))
        
    elif isinstance(json_input, (str, Path)):
        json_path = Path(json_input)
        
        if json_path.is_file():
            # Single JSON file
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            source_name = json_path.stem + '.pdf'
            all_rows.extend(process_single_json(json_data, source_name))
            
        elif json_path.is_dir():
            # Directory of JSON files
            json_files = sorted(json_path.glob('*.json'))
            
            if not json_files:
                print(f"No JSON files found in {json_path}")
                return
            
            print(f"Processing {len(json_files)} JSON file(s)...")
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Use JSON filename (without .json) as source PDF name
                    source_name = json_file.stem + '.pdf'
                    rows = process_single_json(json_data, source_name)
                    all_rows.extend(rows)
                    print(f"  ✓ Processed: {json_file.name} ({len(rows)} rows)")
                    
                except Exception as e:
                    print(f"  ✗ Error processing {json_file.name}: {str(e)}")
                    continue
        else:
            raise ValueError(f"Path does not exist: {json_path}")
    else:
        raise TypeError(f"Invalid input type: {type(json_input)}")
    
    if not all_rows:
        print("No data to write to Excel")
        return
    
    # Create DataFrame from new data
    new_df = pd.DataFrame(all_rows, columns=EXCEL_COLUMNS)
    
    # Check if we should append to existing file
    output_path = Path(output_file)
    
    if append_mode and output_path.exists():
        try:
            # Read existing Excel file
            existing_df = pd.read_excel(output_path, engine='openpyxl')
            
            # Check for duplicate entries based on Unique Reference Number and Item Description
            # to avoid adding the same voucher items twice
            merge_cols = ['Unique Reference Number', 'Description of the Item (Item Name)']
            
            # Identify duplicates
            merged = new_df.merge(
                existing_df[merge_cols], 
                on=merge_cols, 
                how='left', 
                indicator=True
            )
            
            # Keep only new records (not duplicates)
            new_records = new_df[merged['_merge'] == 'left_only']
            
            if len(new_records) == 0:
                print(f"\n⚠ No new records to add (all {len(new_df)} records already exist)")
                return
            
            # Append new records to existing data
            final_df = pd.concat([existing_df, new_records], ignore_index=True)
            
            print(f"\n📊 Append Mode:")
            print(f"  Existing rows: {len(existing_df)}")
            print(f"  New rows added: {len(new_records)}")
            print(f"  Duplicate rows skipped: {len(new_df) - len(new_records)}")
            print(f"  Total rows: {len(final_df)}")
            
        except Exception as e:
            print(f"\n⚠ Error reading existing file: {str(e)}")
            print("  Creating new file instead...")
            final_df = new_df
    else:
        final_df = new_df
        if append_mode:
            print("\n📝 File doesn't exist, creating new file...")
    
    # Write to Excel
    final_df.to_excel(output_file, index=False, engine='openpyxl')
    
    if not append_mode:
        print(f"\n✓ Excel file created successfully: {output_file}")
        print(f"  Total rows: {len(all_rows)}")
        print(f"  Unique vouchers: {final_df['Unique Reference Number'].nunique()}")



def main():
    parser = argparse.ArgumentParser(
        description='Automated pipeline: Download PDFs from Google Drive → Extract JSON via Gemini → Aggregate to Excel'
    )
    parser.add_argument(
        '--drive-folder', '-d',
        required=True,
        help='Google Drive folder ID or URL (must be publicly shared)'
    )
    parser.add_argument(
        '--output', '-o',
        default='combined_output.xlsx',
        help='Output Excel filename (default: combined_output.xlsx)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download step (use existing PDFs in remote_pdfs/)'
    )
    parser.add_argument(
        '--skip-extract',
        action='store_true',
        help='Skip extraction step (use existing JSONs in json_outputs/)'
    )
    parser.add_argument(
        '--append',
        action='store_true',
        help='Append to existing Excel file instead of overwriting'
    )

    args = parser.parse_args()
    
    print('=' * 60)
    print('PDF Processing Pipeline')
    print('=' * 60)
    
    # Step 1: Download PDFs
    if args.skip_download:
        print('\n[1/4] Skipping download (using existing PDFs)...')
        pdf_files = sorted(REMOTE_PDFS_DIR.glob('*.pdf'))
    else:
        folder_id = extract_folder_id(args.drive_folder)
    pdf_files = download_pdfs_from_drive(
        folder_id, 
        REMOTE_PDFS_DIR
    )
    
    if not pdf_files:
        print('No PDF files found. Exiting.')
        sys.exit(1)
    
    # Step 2: Process PDFs with Gemini
    if args.skip_extract:
        print('\n[2/4] Skipping extraction (using existing JSONs)...')
    else:
        process_all_pdfs(pdf_files, JSON_OUTPUTS_DIR)
    
    # Step 3 & 4: Aggregate to Excel
    output_path = SCRIPT_DIR / args.output
    aggregate_to_excel(
        JSON_OUTPUTS_DIR, 
        output_path,
        append_mode=args.append  # Add this parameter
    )
    
    print('\n' + '=' * 60)
    print('Pipeline completed successfully!')
    print('=' * 60)


if __name__ == '__main__':
    main()
