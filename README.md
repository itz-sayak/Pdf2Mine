# PDF to Excel Automation Pipeline

Automated pipeline to download PDFs from Google Drive, extract structured data using Google Gemini API, and aggregate results into a formatted Excel spreadsheet.

## 📋 Overview

This pipeline processes payment voucher PDFs and extracts key information into a standardized 23-column Excel format including:
- Invoice details (number, date, supplier)
- Item descriptions and amounts
- Project information
- Payment categorization
- Administrative approvals

## 🚀 Features

- **Google Drive Integration**: Automatically downloads PDFs from public Drive folders
- **AI-Powered Extraction**: Uses Google Gemini API to extract structured data from PDFs
- **Batch Processing**: Processes multiple PDFs in one run
- **Excel Output**: Generates formatted Excel files with auto-adjusted column widths
- **Flexible Execution**: Skip download or extraction steps using command-line flags
- **Robust Parsing**: Handles multiple JSON structure variations from AI responses

## 📁 Project Structure

```
.
├── pipeline.py              # Main automation script
├── .env                     # API key storage (not in repo)
├── requirements.txt         # Python dependencies
├── remote_pdfs/            # Downloaded PDF files
├── json_outputs/           # Extracted JSON data
└── combined_output.xlsx    # Final aggregated Excel output
```

## 🛠️ Installation

### Prerequisites
- Python 3.8 or higher
- Google Gemini API key ([Get one here](https://makersuite.google.com/app/apikey))

### Setup Steps

1. **Clone or download this repository**

2. **Create virtual environment** (recommended):
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. **Install dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```

4. **Configure API key**:
   
   Create a `.env` file in the project root:
   ```env
   GENAI_API_KEY=your_api_key_here
   ```

## 📖 Usage

### Basic Usage

Process PDFs from a Google Drive folder:
```powershell
python pipeline.py --drive-folder "Drive_folder_id(in the link)" --output combined_output.xlsx
```

### Command-Line Options

```
--drive-folder FOLDER_ID    Google Drive folder ID or full URL (required)
--output FILENAME          Output Excel filename (default: combined_output.xlsx)
--skip-download            Skip downloading PDFs (use existing files in remote_pdfs/)
--skip-extract             Skip PDF extraction (use existing JSONs in json_outputs/)
```

### Examples

**Full pipeline** (download + extract + aggregate):
```powershell
python pipeline.py --drive-folder "Drive_folder_id(in the link)" --output output.xlsx
```

**Skip download** (use existing PDFs):
```powershell
python pipeline.py --drive-folder "Drive_folder_id(in the link)" --output output.xlsx --skip-download
```

**Skip extraction** (use existing JSONs, useful when quota exhausted):
```powershell
python pipeline.py --drive-folder "Drive_folder_id(in the link)" --output output.xlsx --skip-download --skip-extract
```

**Use Drive folder URL** (instead of ID):
```powershell
python pipeline.py --drive-folder "driver link" --output output.xlsx
```

## 📊 Excel Output Format

The generated Excel file contains 23 columns:

| Column | Description |
|--------|-------------|
| Source PDF | Original PDF filename |
| Unique Reference Number | Voucher reference number |
| Invoice No. | Invoice number |
| Invoice Date | Date of invoice |
| Name of the Supplier | Supplier name |
| Payment to be made in the name of | Payee name |
| Purchase Type (Import/Indigenous) | Purchase type |
| Type of Stock (Asset/Cons./Service) | Stock category |
| Subcategory of the stock | Stock subcategory |
| Description of the Item (Item Name) | Item description |
| Net Amount | Net amount per item |
| Remarks | Additional notes |
| Total Amount (INR) | Total voucher amount |
| Advance taken (if any) in INR | Advance amount |
| Less: Penalty Deducted in INR | Penalty deduction |
| Net Amount Payable (figure) INR | Net payable (numeric) |
| Net Amount Payable (words) INR | Net payable (words) |
| Project No | Project number |
| Project Title | Project title |
| Balance in Project | Project balance |
| Overhead Deducted | Overhead deduction |
| Source of payment | Payment source |
| Head of expense | Expense category |

## ⚙️ How It Works

1. **Download**: Uses `gdown` to fetch PDFs from a public Google Drive folder
2. **Upload**: Uploads each PDF to Gemini File API
3. **Extract**: Sends a structured prompt to Gemini to extract voucher data as JSON
4. **Parse**: Handles multiple JSON structure variations (snake_case, PascalCase, nested dicts)
5. **Aggregate**: Combines all JSON files into a single Excel workbook
6. **Format**: Auto-adjusts column widths for readability

## 🔑 Google Drive Setup

For the pipeline to download PDFs automatically:

1. Upload PDFs to a Google Drive folder
2. Right-click the folder → **Share** → **Get link**
3. Set access to **"Anyone with the link"** with **Viewer** permissions
4. Copy the folder ID from the URL:
   ```
   https://drive.google.com/drive/folders/1MxnSyb-XFciLXuJ95Z-Stuyj1hrh15yg
                                            ↑ This is the folder ID
   ```

## ⚠️ API Rate Limits

- **Free Tier**: Gemini API allows ~20 requests per day for `gemini-2.0-flash-exp` model
- If you hit quota limits, use `--skip-extract` to regenerate Excel from existing JSON files
- Consider upgrading to paid tier for higher limits if processing many PDFs

## 🐛 Troubleshooting

### "429 Quota Exceeded" Error
You've hit the daily API limit. Options:
- Wait 24 hours for quota reset
- Use `--skip-extract` flag to work with existing JSON files
- Upgrade to paid API tier

### "403 Permission Denied" on Drive Download
Ensure the Drive folder is shared publicly with "Anyone with the link" access.

### "ModuleNotFoundError"
Install dependencies:
```powershell
pip install -r requirements.txt
```

### Empty Excel or Missing Data
- Check that PDFs contain payment voucher information
- Verify JSON files in `json_outputs/` directory
- Review Gemini API response for errors

## 📝 Notes

- The pipeline creates `remote_pdfs/` and `json_outputs/` directories automatically
- JSON files are preserved for debugging and reprocessing
- Excel columns are auto-sized based on content
- Supports multiple items per voucher (creates separate rows)

## 🔒 Security

- Store API key in `.env` file (add to `.gitignore`)
- Never commit `.env` to version control
- Use read-only Drive access (Viewer permissions)

## 📄 License

This project is provided as-is for automation purposes.

---

**Last Updated**: December 10, 2025  
**Python Version**: 3.8+  
**Gemini Model**: gemini-2.0-flash-exp
