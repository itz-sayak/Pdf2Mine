#!/usr/bin/env python
"""
Automated Pipeline: Download PDFs from Google Drive -> Extract JSON via Gemini -> Aggregate to Excel

Usage:
    python pipeline.py --drive-folder <FOLDER_ID_OR_URL> --output combined_output.xlsx

Example:
    python pipeline.py --drive-folder 1MxnSyb-XFciLXuJ95Z-Stuyj1hrh15yg --output combined_output.xlsx
    python pipeline.py --drive-folder "https://drive.google.com/drive/folders/1MxnSyb-XFciLXuJ95Z-Stuyj1hrh15yg" --output combined_output.xlsx

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

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

import gdown
import google.generativeai as genai
from openpyxl import Workbook

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
REMOTE_PDFS_DIR = SCRIPT_DIR / 'remote_pdfs'
JSON_OUTPUTS_DIR = SCRIPT_DIR / 'json_outputs'
SAMPLE_PDF = SCRIPT_DIR / 'sample.pdf'

PROMPT = """
Extract all content from this PDF document and convert it into structured JSON format.
Include all text, tables, and important information.
Organize the data logically with appropriate keys and nested structures.
Return ONLY valid JSON, no markdown formatting or additional text.
"""

EXCEL_COLUMNS = [
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

def extract_folder_id(folder_input: str) -> str:
    """Extract folder ID from URL or return as-is if already an ID."""
    match = re.search(r'folders/([A-Za-z0-9_-]+)', folder_input)
    if match:
        return match.group(1)
    # Assume it's already a folder ID
    return folder_input.strip()


def download_pdfs_from_drive(folder_id: str, output_dir: Path) -> list:
    """Download all files from a public Google Drive folder using gdown."""
    output_dir.mkdir(exist_ok=True)
    
    # Clean previous downloads
    for f in output_dir.glob('*'):
        if f.is_file():
            f.unlink()
    
    url = f'https://drive.google.com/drive/folders/{folder_id}'
    print(f'\n[1/4] Downloading PDFs from Google Drive folder: {url}')
    
    try:
        gdown.download_folder(url, output=str(output_dir), quiet=False)
    except Exception as e:
        print(f'Error downloading folder: {e}')
        return []
    
    pdf_files = sorted(output_dir.glob('*.pdf'))
    print(f'Downloaded {len(pdf_files)} PDF(s): {[f.name for f in pdf_files]}')
    return pdf_files


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


def aggregate_to_excel(json_dir: Path, output_xlsx: Path):
    """Read all JSON files and aggregate into a single Excel workbook."""
    print(f'\n[3/4] Aggregating JSON outputs to Excel...')
    
    wb = Workbook()
    ws = wb.active
    ws.title = 'combined'
    ws.append(EXCEL_COLUMNS)
    
    json_files = sorted(json_dir.glob('*.json'))
    if not json_files:
        print('No JSON files found to aggregate.')
        return
    
    total_rows = 0
    for fp in json_files:
        name = fp.stem
        print(f'  Reading: {fp.name}')
        
        try:
            text = fp.read_text(encoding='utf-8')
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = text
            
            rows = build_rows_from_parsed(parsed, name)
            for r in rows:
                # Sanitize dict/list cells
                sanitized = []
                for v in r:
                    if isinstance(v, dict):
                        # If dict has 'selected' key, extract that instead
                        if 'selected' in v:
                            sanitized.append(v['selected'])
                        else:
                            try:
                                sanitized.append(json.dumps(v, ensure_ascii=False))
                            except Exception:
                                sanitized.append(str(v))
                    elif isinstance(v, list):
                        try:
                            sanitized.append(json.dumps(v, ensure_ascii=False))
                        except Exception:
                            sanitized.append(str(v))
                    else:
                        sanitized.append(v)
                ws.append(sanitized)
                total_rows += 1
        except Exception as e:
            print(f'  Error reading {fp.name}: {e}')
    
    # Adjust column widths
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                length = len(str(cell.value))
                if length > max_length:
                    max_length = length
        ws.column_dimensions[col_letter].width = min(50, max(10, max_length + 2))
    
    wb.save(output_xlsx)
    print(f'\n[4/4] ✓ Saved: {output_xlsx} ({total_rows} data rows)')


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
        pdf_files = download_pdfs_from_drive(folder_id, REMOTE_PDFS_DIR)
    
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
    aggregate_to_excel(JSON_OUTPUTS_DIR, output_path)
    
    print('\n' + '=' * 60)
    print('Pipeline completed successfully!')
    print('=' * 60)


if __name__ == '__main__':
    main()
