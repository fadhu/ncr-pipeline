# NCR Processing Pipeline

> Multi-agent AI pipeline that converts Non-Conformance Reports into structured, categorised CAPA tables.

## Overview

This project automates the processing of construction Non-Conformance Reports (NCRs) from PMWeb or local file storage. It uses a 6-agent AI pipeline powered by Claude to:

1. **Ingest** NCR files (PDF, Word, scanned images) from a local folder
2. **Parse** and normalise messy document text (OCR, mixed layouts, form fields)
3. **Classify** the non-conformance issue into a predefined taxonomy
4. **Tag corrective actions** — immediate fixes applied
5. **Tag preventive actions** — systemic changes to prevent recurrence
6. **Export** a structured, colour-coded Excel CAPA report with analytics

## Project Structure

```
ncr-pipeline/
├── backend/
│   ├── ncr_pipeline.py       # Main pipeline (6 agents + Excel exporter)
│   ├── requirements.txt      # Python dependencies
│   ├── .env.example           # Environment variable template
│   └── README.md              # Backend documentation
├── .gitignore
└── README.md                  # This file
```

## Quick Start

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/ncr-pipeline.git
cd ncr-pipeline/backend

# Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
sudo apt install tesseract-ocr poppler-utils  # System deps

# Configure
cp .env.example .env
# Add your ANTHROPIC_API_KEY to .env

# Run
python ncr_pipeline.py --input-folder /path/to/ncr-files --output report.xlsx
```

## Requirements

- Python 3.11+
- Anthropic API key (Claude Sonnet 4)
- Tesseract OCR + Poppler (for PDF/image processing)

## License

MIT
