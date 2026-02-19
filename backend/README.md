# NCR Pipeline — Backend

Multi-agent AI pipeline that processes Non-Conformance Reports (NCRs) from local files and outputs a structured CAPA (Corrective and Preventive Action) table.

## Architecture

```
Local Folder (PDF / DOCX / Images)
        │
        ▼
┌─────────────────────┐
│ 1. File Ingestion    │  pdfplumber · python-docx · Tesseract OCR
│    Agent             │  Auto-detects scanned PDFs, falls back to OCR
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 2. Document Parser   │  Claude Sonnet 4 · Vision API for scanned forms
│    Agent             │  Extracts: NCR ID, dates, issue, response, closeout
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 3. Issue Classifier  │  Claude Sonnet 4 · Taxonomy classification
│    Agent             │  → Issue category + subcategory + confidence
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 4. Corrective Action │  Claude Sonnet 4 · CAPA framework
│    Tagger            │  → CA category + actions taken + confidence
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 5. Preventive Action │  Claude Sonnet 4 · Root-cause mapping
│    Tagger            │  → PA category + recommendations + confidence
└────────┬────────────┘
         ▼
┌─────────────────────┐
│ 6. Table Structurer  │  Pydantic validation · openpyxl export
│    & Exporter        │  → Colour-coded Excel with summary analytics
└─────────────────────┘
```

## Supported File Types

| Format | Extraction Method |
|--------|-------------------|
| PDF (text) | `pdfplumber` — text + tables |
| PDF (scanned) | Auto-detected → `pdf2image` + Tesseract OCR |
| Word (.docx) | `python-docx` — paragraphs + tables |
| Images (.jpg, .png, .tiff) | Tesseract OCR + Claude Vision |

## Setup

### System Dependencies

```bash
# Ubuntu / Debian
sudo apt install tesseract-ocr poppler-utils

# macOS
brew install tesseract poppler
```

### Python Dependencies

```bash
cd backend
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Environment Variables

```bash
cp .env.example .env
# Edit .env and add your Anthropic API key
```

## Usage

### CLI

```bash
# Basic usage
python ncr_pipeline.py --input-folder ./ncr_files --output ncr_report.xlsx

# With concurrency control
python ncr_pipeline.py -i ./ncr_files -o report.xlsx --concurrency 5
```

### Programmatic

```python
import asyncio
from ncr_pipeline import NCRPipeline

pipeline = NCRPipeline(anthropic_api_key="sk-ant-...")
results = asyncio.run(pipeline.run("./ncr_files", concurrency=3))
pipeline.export(results, "ncr_report.xlsx")
```

## Output

The pipeline produces an Excel workbook with two sheets:

### Sheet 1: NCR CAPA Report
Each NCR file becomes a row with these columns:

| Column | Description |
|--------|-------------|
| NCR ID | Extracted reference number |
| Source File | Original filename |
| Project | Project name |
| Date Raised | NCR open date (YYYY-MM-DD) |
| Date Resolved | NCR close date or "OPEN" |
| Days Open | Calculated duration (red if >30 days) |
| Discipline | Engineering discipline |
| Location | Area / zone / grid reference |
| Issue Description | AI-summarised non-conformance |
| Issue Category | Classified tag (e.g., Material Defect) |
| Corrective Action | AI-extracted immediate fix |
| CA Category | Classified tag (e.g., Rework / Repair) |
| Preventive Action | AI-extracted or recommended systemic fix |
| PA Category | Classified tag (e.g., Training / Competency) |
| Confidence | Average confidence score across agents |

### Sheet 2: Summary
- Issue category breakdown with counts and percentages
- Resolution time analytics (avg / min / max days)
- Open NCRs detail table (sorted by longest-open)

## Taxonomy

### Issue Categories
- Material Defect
- Workmanship Error
- Design Deviation
- Specification Non-Compliance
- Safety Violation
- Environmental Non-Compliance
- Documentation Gap
- Process Deviation

### Corrective Action Categories
- Rework / Repair
- Replacement
- Reinspection
- Use-As-Is (with concession)
- Reject / Scrap
- Design Revision
- Immediate Containment

### Preventive Action Categories
- Process Update / SOP Revision
- Training / Competency
- Supplier Corrective Action
- Design Change
- Inspection Enhancement
- Root Cause Elimination
- System / Tool Upgrade

## Customisation

Edit the `IssueCategory`, `CorrectiveActionCategory`, and `PreventiveActionCategory` enums in `ncr_pipeline.py` to match your organisation's taxonomy.
