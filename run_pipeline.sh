#!/bin/bash
# run_pipeline.sh - Automated PDF to Excel Pipeline Script
# This script runs the payment voucher processing pipeline

# =============================================================================
# CONFIGURATION - UPDATE THESE VARIABLES
# =============================================================================

# Full path to your Python executable (use 'which python' or 'which python3' to find)
PYTHON_PATH="/usr/bin/python3"

# Full path to the directory containing pipeline.py
PROJECT_DIR="PROJECT_DIR"

# Google Drive folder ID (extract from your Drive folder URL)
DRIVE_FOLDER_ID="your_drive_folder_id_here"

# Output Excel file name
OUTPUT_FILE="combined_output.xlsx"

# Log file location (for tracking execution)
LOG_DIR="${PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/pipeline_$(date +%Y%m%d_%H%M%S).log"

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================

# Create logs directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Start logging
echo "========================================" | tee -a "${LOG_FILE}"
echo "Pipeline Execution Started: $(date)" | tee -a "${LOG_FILE}"
echo "========================================" | tee -a "${LOG_FILE}"

# Change to project directory
cd "${PROJECT_DIR}" || {
    echo "ERROR: Cannot change to directory ${PROJECT_DIR}" | tee -a "${LOG_FILE}"
    exit 1
}

# Activate virtual environment if you're using one (uncomment if needed)
# source venv/bin/activate

# Run the pipeline
echo "Running pipeline..." | tee -a "${LOG_FILE}"
"${PYTHON_PATH}" pipeline.py \
    --drive-folder "${DRIVE_FOLDER_ID}" \
    --output "${OUTPUT_FILE}" \
    2>&1 | tee -a "${LOG_FILE}"

# Check exit status
EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 0 ]; then
    echo "✓ Pipeline completed successfully" | tee -a "${LOG_FILE}"
else
    echo "✗ Pipeline failed with exit code ${EXIT_CODE}" | tee -a "${LOG_FILE}"
fi

echo "========================================" | tee -a "${LOG_FILE}"
echo "Pipeline Execution Ended: $(date)" | tee -a "${LOG_FILE}"
echo "========================================" | tee -a "${LOG_FILE}"

# Optional: Send notification (uncomment if needed)
# if [ ${EXIT_CODE} -eq 0 ]; then
#     echo "Pipeline completed" | mail -s "Pipeline Success" your-email@example.com
# else
#     echo "Pipeline failed" | mail -s "Pipeline Failed" your-email@example.com
# fi

exit ${EXIT_CODE}
