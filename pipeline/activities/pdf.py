"""
pipeline/activities/pdf.py

Two activities:
  1. assess_pdf_quality  - decide which extraction strategy to use
  2. extract_pdf_text    - run the chosen strategy and return markdown text

Why two separate activities instead of one?
- Because quality assessment and extraction have different timeout needs. 
- Assessment samples 5 pages and finishes in seconds. Extraction on a 100-page scanned PDF with OCR can take 20 minutes.
- Separating them means you can set tight timeouts on assessment and generous timeouts on extraction without one affecting the other. It also means if extraction fails and retries, you don't re-run the quality check
"""
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import fitz          # PyMuPDF
import pymupdf4llm
from temporalio import activity

from config import (
    MIN_CHARS_PER_PAGE,
    MAX_NON_ASCII_RATIO,
    MIN_TEXT_PAGE_RATIO,
)


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class PDFQualityInput:
    local_path: str


@dataclass
class PDFQualityOutput:
    strategy: str            # 'direct' | 'ocr' | 'mixed'
    quality_score: float     # 0.0 – 1.0
    page_count: int
    warning: Optional[str]


@dataclass
class ExtractPDFInput:
    local_path: str
    strategy: str            # from PDFQualityOutput
    ## The workflow sets heartbeat_timeout=45s 
    ## For typical CUAD contracts, 5 pages extracts in 2-10 seconds well within the window. The heartbeat after each batch keeps Temporal informed.
    batch_size: int = 5      # pages per batch (heartbeat cadence)


@dataclass
class ExtractPDFOutput:
    full_text: str
    page_count: int
    char_count: int
    strategy_used: str


# ── Activity 1: Quality assessment ───────────────────────────────────────────

@activity.defn
async def assess_pdf_quality(params: PDFQualityInput) -> PDFQualityOutput:
    """
    Samples up to 5 pages to decide the best extraction strategy:
      - 'direct' : native text layer is clean — use pymupdf4llm
      - 'ocr'    : scanned or garbled — use Tesseract
      - 'mixed'  : some pages have text, some are images — route page by page
    """
    activity.heartbeat({"stage": "assessing_quality", "path": params.local_path})

    doc = fitz.open(params.local_path)
    page_count = doc.page_count

    sample_pages = min(5, page_count)
    total_chars = 0
    non_ascii   = 0
    pages_with_text = 0

    for i in range(sample_pages):
        text = doc[i].get_text()
        total_chars    += len(text)
        non_ascii      += sum(1 for c in text if ord(c) > 127)
        if len(text.strip()) > 50:
            pages_with_text += 1

    # text density per page
    avg_chars   = total_chars / sample_pages if sample_pages else 0
    # ratio of garbled characters
    non_ascii_r = non_ascii / total_chars    if total_chars  else 1.0
    # fraction of pages with text
    text_cov    = pages_with_text / sample_pages if sample_pages else 0

    # avg_chars < 100 → "ocr"    score=0.2  (scanned PDF, no text layer)
    if avg_chars < MIN_CHARS_PER_PAGE:
        strategy = "ocr"
        score    = 0.2
        warning  = (f"Low text density ({avg_chars:.0f} chars/page) — "
                    f"PDF appears scanned")
    # non_ascii_ratio > 0.3 → "ocr"    score=0.4  (encoding problem)
    elif non_ascii_r > MAX_NON_ASCII_RATIO:
        strategy = "ocr"
        score    = 0.4
        warning  = (f"High non-ASCII ratio ({non_ascii_r:.0%}) — "
                    f"possible encoding issue")
    # text_coverage < 0.5 → "mixed"  score=0.6  (some pages scanned, some not)
    elif text_cov < MIN_TEXT_PAGE_RATIO:
        strategy = "mixed"
        score    = 0.6
        warning  = (f"Only {text_cov:.0%} of sampled pages have extractable text")
    else:
        # otherwise → "direct" score=0.0-1.0 (clean native text)
        strategy = "direct"
        score    = min(1.0, avg_chars / 1000)
        warning  = None

    activity.logger.info(
        f"Quality check: strategy={strategy}, score={score:.2f}, "
        f"avg_chars/page={avg_chars:.0f}, pages={page_count}"
    )
    if warning:
        activity.logger.warning(f"Quality warning: {warning}")

    return PDFQualityOutput(
        strategy=strategy,
        quality_score=score,
        page_count=page_count,
        warning=warning,
    )


# ── Activity 2: Text extraction ───────────────────────────────────────────────

@activity.defn
async def extract_pdf_text(params: ExtractPDFInput) -> ExtractPDFOutput:
    """Dispatch to the right extraction strategy."""
    if params.strategy == "ocr":
        return await _extract_ocr(params)
    elif params.strategy == "mixed":
        return await _extract_mixed(params)
    else:
        return await _extract_direct(params)


## Why pymupdf4llm instead of plain PyMuPDF?
## Plain PyMuPDF gives you raw text no formatting, tables become garbled, headers are indistinguishable from body text. 
## pymupdf4llm converts to markdown tables become markdown tables, headers get # prefixes, bold text is preserved.

async def _extract_direct(params: ExtractPDFInput) -> ExtractPDFOutput:
    """
    Native text extraction via pymupdf4llm.
    Processes in batches so we can heartbeat progress.
    """
    doc = fitz.open(params.local_path)
    total_pages = doc.page_count
    num_batches = math.ceil(total_pages / params.batch_size)
    chunks = []

    for batch_idx in range(num_batches):
        start = batch_idx * params.batch_size
        end   = min(start + params.batch_size, total_pages)

        batch_md = pymupdf4llm.to_markdown(
            params.local_path,
            pages=list(range(start, end)),
        )
        chunks.append(batch_md)

        activity.heartbeat({
            "stage":        "extracting_direct",
            "pages_done":   end,
            "total_pages":  total_pages,
            "progress_pct": round(end / total_pages * 100),
        })

    full_text = "\n\n".join(chunks)
    return ExtractPDFOutput(
        full_text=full_text,
        page_count=total_pages,
        char_count=len(full_text),
        strategy_used="direct",
    )


async def _extract_ocr(params: ExtractPDFInput) -> ExtractPDFOutput:
    """
    OCR via pytesseract + pdf2image.
    Falls back to direct extraction if OCR libraries are not installed.
    """
    try:
        import pytesseract
        ## converts each PDF page to a high-resolution image at 300 DPI 
        from pdf2image import convert_from_path
    except ImportError:
        activity.logger.warning(
            "OCR libraries (pytesseract, pdf2image) not installed — "
            "falling back to direct extraction"
        )
        return await _extract_direct(
            ExtractPDFInput(
                local_path=params.local_path,
                strategy="direct",
                batch_size=params.batch_size,
            )
        )

    images = convert_from_path(params.local_path, dpi=300)
    pages_text = []

    for i, image in enumerate(images):
        text = pytesseract.image_to_string(image, lang="eng")
        pages_text.append(text)
        activity.heartbeat({
            "stage":        "extracting_ocr",
            "page":         i + 1,
            "total_pages":  len(images),
            "progress_pct": round((i + 1) / len(images) * 100),
        })

    full_text = "\n\n".join(pages_text)
    return ExtractPDFOutput(
        full_text=full_text,
        page_count=len(images),
        char_count=len(full_text),
        strategy_used="ocr",
    )

## page by page routing
async def _extract_mixed(params: ExtractPDFInput) -> ExtractPDFOutput:
    """
    Page-by-page routing:
      pages with native text → pymupdf4llm
      image pages            → pytesseract
    """
    try:
        import pytesseract
        from PIL import Image
        import io
        has_ocr = True
    except ImportError:
        has_ocr = False
        activity.logger.warning("OCR not available — using direct for all pages")

    doc = fitz.open(params.local_path)
    pages_text = []

    for i in range(doc.page_count):
        page = doc[i]
        text = page.get_text().strip()

        ## Even image pages sometimes have a tiny amount of text from PyMuPDF page numbers, headers extracted from PDF metadata. 50 characters filters out those false positives. A real text page always has much more.
        if len(text) > 50:
            # Good native text
            md = pymupdf4llm.to_markdown(params.local_path, pages=[i])
            pages_text.append(md)
        elif has_ocr:
            # Image page — OCR it
            pix = page.get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            ocr_text = pytesseract.image_to_string(img, lang="eng")
            pages_text.append(ocr_text)
        else:
            pages_text.append("")   # can't extract — skip

        activity.heartbeat({
            "stage":        "extracting_mixed",
            "page":         i + 1,
            "total_pages":  doc.page_count,
            "progress_pct": round((i + 1) / doc.page_count * 100),
        })

    full_text = "\n\n".join(pages_text)
    ## strategy_used is stored in Postgres so you can later query: "how many contracts needed OCR?" and "do OCR contracts have lower eval recall?" useful for understanding your data quality.
    return ExtractPDFOutput(
        full_text=full_text, # full markdown string, ready for LLM
        page_count=doc.page_count,
        char_count=len(full_text),
        strategy_used="mixed", #  # records which strategy was actually used

    )