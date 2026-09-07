"""Email Pending Verification: PDF extract, previous-day filter, Excel match."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Iterable

import pandas as pd


DATE_PATTERNS = (
    r"\d{1,2}\s*[/\-\.]\s*\d{1,2}\s*[/\-\.]\s*\d{2,4}",
    r"\d{4}\s*-\s*\d{1,2}\s*-\s*\d{1,2}",
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}",
    r"\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{2,4}",
)
DATE_TOKEN_RE = re.compile("|".join(DATE_PATTERNS), re.IGNORECASE)
PRINTED_LABEL_RE = re.compile(r"printt?ed\s*on\s*:?", re.IGNORECASE)
PRINTED_ON_RE = re.compile(
    r"printt?ed\s*on\s*:?\s*(" + "|".join(DATE_PATTERNS) + r")",
    re.IGNORECASE,
)
DOCTOR_RE = re.compile(
    r"\bDr\.?\s+(.+)$",
    re.IGNORECASE,
)
FILENAME_SKIP_WORDS = {
    "report",
    "daily",
    "allocation",
    "pending",
    "verification",
    "file",
    "print",
    "printed",
}
TITLE_STRIP_RE = re.compile(
    r"\b(dr\.?|dds|dmd|doctor)\b",
    re.IGNORECASE,
)


def extract_doctor_name_from_filename(filename: str) -> str:
    """Pull doctor name from 'Dr. <name>' or 'Dr <name>' in the PDF filename."""
    base = os.path.basename(str(filename or "")).strip()
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = base.replace("_", " ").replace("-", " ")
    base = re.sub(r"\s+", " ", base).strip()
    match = DOCTOR_RE.search(base)
    if not match:
        return ""
    parts = []
    for part in match.group(1).split():
        cleaned = part.strip(" ._")
        if not cleaned:
            continue
        if cleaned.lower() in FILENAME_SKIP_WORDS:
            break
        if re.fullmatch(r"\d{1,4}", cleaned):
            break
        parts.append(cleaned)
    return " ".join(parts)


def parse_flexible_date(value: Any):
    """Parse common US and ISO date strings. Two-digit years use 2000+."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date()
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "nat"):
        return None
    match = re.search("|".join(DATE_PATTERNS), text)
    if not match:
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()
    token = match.group(0)
    cleaned = re.sub(r",", "", token)
    for fmt in (
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%m.%d.%Y",
        "%m.%d.%y",
        "%Y-%m-%d",
        "%b %d %Y",
        "%B %d %Y",
        "%d %b %Y",
        "%d %B %Y",
        "%d-%b-%Y",
        "%d-%b-%y",
    ):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    parsed = pd.to_datetime(token, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def format_display_date(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d/%Y")
    return str(value)


def previous_calendar_date(printed_on):
    if printed_on is None:
        return None
    return printed_on - timedelta(days=1)


def _normalize_pdf_text(text: str) -> str:
    cleaned = (
        str(text or "")
        .replace("\xa0", " ")
        .replace("\u00a0", " ")
        .replace("\u2007", " ")
        .replace("\u202f", " ")
    )
    cleaned = re.sub(r"(\d)\s*[/\-\.]\s*(\d)", r"\1/\2", cleaned)
    return cleaned


def extract_printed_on_date(text: str):
    """Read the date in front of or just after Printed/Printted On:."""
    if not text:
        return None
    collapsed = re.sub(r"[ \t]+", " ", _normalize_pdf_text(text))
    for match in PRINTED_LABEL_RE.finditer(collapsed):
        after = collapsed[match.end() : match.end() + 120]
        found = DATE_TOKEN_RE.search(after)
        if found:
            parsed = parse_flexible_date(found.group(0))
            if parsed:
                return parsed
        before = collapsed[max(0, match.start() - 120) : match.start()]
        found_before = list(DATE_TOKEN_RE.finditer(before))
        if found_before:
            parsed = parse_flexible_date(found_before[-1].group(0))
            if parsed:
                return parsed
    match = PRINTED_ON_RE.search(collapsed)
    if match:
        return parse_flexible_date(match.group(1))
    return None


def _is_printed_label_word(value: str) -> bool:
    text = str(value or "").strip().lower().rstrip(":.")
    return text in ("printed", "printted", "printedon", "printtedon")


def _is_on_label_word(value: str) -> bool:
    return re.fullmatch(r"on", str(value or "").strip().lower().rstrip(":.")) is not None


def _parse_date_from_joined_words(texts) -> Any:
    joined = _normalize_pdf_text(" ".join(str(t) for t in texts if t))
    compact = re.sub(r"\s+", "", joined)
    spaced = re.sub(r"\s+", " ", joined)
    for candidate in (spaced, compact, joined):
        found = DATE_TOKEN_RE.search(candidate)
        if found:
            parsed = parse_flexible_date(found.group(0))
            if parsed:
                return parsed
    return None


def extract_printed_on_from_words(words) -> Any:
    """Use PDF word positions so the date beside Printed On is not missed."""
    if not words:
        return None
    normalized = []
    for word in words:
        text = str(word.get("text") or "").strip()
        if not text:
            continue
        normalized.append(
            {
                "text": text,
                "x0": float(word.get("x0") or 0),
                "x1": float(word.get("x1") or 0),
                "top": float(word.get("top") or 0),
            }
        )
    for idx, word in enumerate(normalized):
        if not _is_printed_label_word(word["text"]):
            continue
        on_word = None
        combined_label = "printedon" in word["text"].lower().replace(" ", "").replace(":", "")
        if not combined_label:
            for nxt in normalized[idx + 1 : idx + 8]:
                if abs(nxt["top"] - word["top"]) > 22:
                    continue
                if _is_on_label_word(nxt["text"]):
                    on_word = nxt
                    break
        label_right = on_word["x1"] if on_word else word["x1"]
        label_left = word["x0"]
        label_top = word["top"]
        nearby = [
            item
            for item in normalized
            if item["top"] >= label_top - 8 and item["top"] <= label_top + 28
        ]
        nearby.sort(key=lambda item: (item["top"], item["x0"]))
        right_words = [
            item["text"]
            for item in nearby
            if item["x0"] >= label_right - 4
        ]
        parsed = _parse_date_from_joined_words(right_words)
        if parsed:
            return parsed
        left_words = [
            item["text"]
            for item in nearby
            if item["x1"] <= label_left + 4
        ]
        parsed = _parse_date_from_joined_words(left_words)
        if parsed:
            return parsed
    return None


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = TITLE_STRIP_RE.sub(" ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def names_match(left: Any, right: Any) -> bool:
    a = normalize_name(left)
    b = normalize_name(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        return True
    a_parts = a.split()
    b_parts = b.split()
    if len(a_parts) >= 2 and len(b_parts) >= 2:
        if a_parts[0] == b_parts[-1] and a_parts[-1] == b_parts[0]:
            return True
        if a_parts[-1] == b_parts[-1] and a_parts[0][0] == b_parts[0][0]:
            return True
    return False


def _normalize_header(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\n", " ").strip().lower())


def _is_patient_header(value: str) -> bool:
    text = _normalize_header(value)
    if not text:
        return False
    if text in ("patient", "patient name", "pt", "pt name", "pt. name"):
        return True
    if text.startswith("patient") and not any(
        token in text for token in ("id", "account", "number", "phone", "dob")
    ):
        return True
    return False


def _is_entered_header(value: str) -> bool:
    """Date column 'Entered' / 'Date Entered'. Never 'Entered By'."""
    text = _normalize_header(value)
    if not text or "by" in text.split():
        return False
    tokens = text.split()
    return tokens[0] == "entered" or tokens[-1] == "entered"


def _looks_like_name_header(value: str) -> bool:
    if _is_patient_header(value):
        return True
    text = _normalize_header(value)
    return text in ("name",) or (
        "name" in text and "agent" not in text and "office" not in text
    )


def _looks_like_date_header(value: str) -> bool:
    if _is_entered_header(value):
        return True
    text = _normalize_header(value)
    return "date" in text or text in ("dos", "appt", "appointment")


def _find_patient_entered_header(table):
    """Return (body_start, headers) for a table that has Patient and Entered."""
    if not table:
        return None, None
    for idx, raw in enumerate(table):
        headers = [str(cell or "").replace("\n", " ").strip() for cell in (raw or [])]
        if any(_is_patient_header(h) for h in headers) and any(
            _is_entered_header(h) for h in headers
        ):
            return idx + 1, headers
        if idx + 1 < len(table):
            nxt = table[idx + 1] or []
            merged = []
            width = max(len(headers), len(nxt))
            for col in range(width):
                left = headers[col] if col < len(headers) else ""
                right = (
                    str(nxt[col] or "").replace("\n", " ").strip()
                    if col < len(nxt)
                    else ""
                )
                merged.append((left + " " + right).strip())
            if any(_is_patient_header(h) for h in merged) and any(
                _is_entered_header(h) for h in merged
            ):
                return idx + 2, merged
    return None, None


def _iter_pdf_tables(page):
    """Try a few table strategies so Patient/Entered columns are found."""
    settings_list = (
        None,
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
    )
    seen = set()
    for settings in settings_list:
        tables = (
            page.extract_tables()
            if settings is None
            else page.extract_tables(table_settings=settings)
        )
        for table in tables or []:
            key = tuple(
                tuple(str(cell or "") for cell in (row or [])) for row in table[:4]
            )
            if key in seen:
                continue
            seen.add(key)
            yield table


def _normalize_words(words):
    normalized = []
    for word in words or []:
        text = str(word.get("text") or "").strip()
        if not text:
            continue
        normalized.append(
            {
                "text": text,
                "x0": float(word.get("x0") or 0),
                "x1": float(word.get("x1") or 0),
                "top": float(word.get("top") or 0),
            }
        )
    return normalized


def _cluster_word_rows(words, tolerance=5):
    rows = []
    for word in sorted(words, key=lambda item: (item["top"], item["x0"])):
        if rows and abs(word["top"] - rows[-1]["top"]) <= tolerance:
            rows[-1]["words"].append(word)
        else:
            rows.append({"top": word["top"], "words": [word]})
    return rows


def _find_patient_entered_columns(words):
    """Locate Patient and Entered header x-ranges from a page's words.

    Alerts Report order is Date | Entered | Patient | ... | Entered By.
    """
    row_groups = _cluster_word_rows(_normalize_words(words), tolerance=8)
    for group in row_groups:
        patient_word = None
        entered_words = []
        for word in group["words"]:
            text = word["text"]
            if patient_word is None and _is_patient_header(text):
                patient_word = word
            if _is_entered_header(text):
                entered_words.append(word)
        if not patient_word or not entered_words:
            continue
        left_of_patient = [w for w in entered_words if w["x0"] < patient_word["x0"]]
        entered_word = max(left_of_patient, key=lambda w: w["x0"]) if left_of_patient else min(
            entered_words, key=lambda w: abs(w["x0"] - patient_word["x0"])
        )
        markers = sorted(
            [("entered", entered_word), ("patient", patient_word)],
            key=lambda item: item[1]["x0"],
        )
        others = [
            word
            for word in group["words"]
            if word is not patient_word and word is not entered_word
        ]
        ranges = {}
        for idx, (name, marker) in enumerate(markers):
            start = marker["x0"] - 10
            next_x = None
            if idx + 1 < len(markers):
                next_x = markers[idx + 1][1]["x0"]
            else:
                after = [w["x0"] for w in others if w["x0"] > marker["x0"] + 8]
                if after:
                    next_x = min(after)
            end = (next_x - 4) if next_x is not None else marker["x1"] + 90
            if end <= start:
                end = start + 80
            ranges[name] = (start, end)
        ranges["header_top"] = group["top"]
        return ranges
    return None


def _extract_patient_entered_from_words(words, columns, skip_header=True) -> list[dict]:
    if not columns:
        return []
    rows = []
    for group in _cluster_word_rows(_normalize_words(words), tolerance=5):
        if skip_header and abs(group["top"] - columns["header_top"]) <= 6:
            continue
        patient_bits = []
        entered_bits = []
        for word in group["words"]:
            mid = (word["x0"] + word["x1"]) / 2
            if columns["patient"][0] <= mid < columns["patient"][1]:
                patient_bits.append(word["text"])
            elif columns["entered"][0] <= mid <= columns["entered"][1]:
                entered_bits.append(word["text"])
        patient = re.sub(r"\s+", " ", " ".join(patient_bits)).strip()
        entered = _parse_date_from_joined_words(entered_bits)
        if not patient or entered is None:
            continue
        if _is_patient_header(patient) or _is_entered_header(patient):
            continue
        rows.append(
            {
                "Patient": patient,
                "Entered": format_display_date(entered),
            }
        )
    return rows


def _collect_from_word_pages(text_parts, word_sets):
    printed_on = extract_printed_on_date("\n".join(text_parts))
    header_columns = None
    word_rows = []
    for words in word_sets:
        if printed_on is None:
            printed_on = extract_printed_on_from_words(words)
        page_columns = _find_patient_entered_columns(words)
        if page_columns:
            header_columns = page_columns
        if header_columns:
            word_rows.extend(
                _extract_patient_entered_from_words(
                    words,
                    header_columns,
                    skip_header=page_columns is not None,
                )
            )
    return "\n".join(text_parts), word_rows, printed_on


def _extract_with_pymupdf(path: str):
    try:
        import fitz
    except ImportError:
        return "", [], None
    texts = []
    word_sets = []
    doc = fitz.open(path)
    for page in doc:
        texts.append(page.get_text("text") or "")
        words = []
        for x0, top, x1, _bottom, text, *_rest in page.get_text("words"):
            token = str(text or "").strip()
            if token:
                words.append({"text": token, "x0": float(x0), "x1": float(x1), "top": float(top)})
        word_sets.append(words)
    if not any(texts) and not any(word_sets):
        return "", [], None
    return _collect_from_word_pages(texts, word_sets)


def _configure_tesseract():
    try:
        import shutil

        import pytesseract

        found = shutil.which("tesseract")
        for candidate in ("/opt/homebrew/bin/tesseract", "/usr/local/bin/tesseract"):
            if found:
                break
            if os.path.exists(candidate):
                found = candidate
        if found:
            pytesseract.tesseract_cmd = found
        pytesseract.get_tesseract_version()
        return True
    except Exception:
        return False


def _tesseract_ready() -> bool:
    return _configure_tesseract()


def _ocr_pdf_pages(path: str):
    """Render outlined Print-to-PDF pages and OCR Printed On / Patient / Entered."""
    import io

    import fitz
    import pytesseract
    from PIL import Image

    texts = []
    word_sets = []
    doc = fitz.open(path)
    scale = 2.0
    for page in doc:
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        image = Image.open(io.BytesIO(pix.tobytes("png")))
        texts.append(pytesseract.image_to_string(image) or "")
        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
        words = []
        for idx, token in enumerate(data.get("text") or []):
            token = str(token or "").strip()
            if not token:
                continue
            try:
                conf = float(data["conf"][idx])
            except (TypeError, ValueError, KeyError):
                conf = 100
            if conf < 25:
                continue
            left = float(data["left"][idx]) / scale
            top = float(data["top"][idx]) / scale
            width = float(data["width"][idx]) / scale
            words.append(
                {
                    "text": token,
                    "x0": left,
                    "x1": left + width,
                    "top": top,
                }
            )
        word_sets.append(words)
    return _collect_from_word_pages(texts, word_sets)


def _row_from_table(headers: list, values: list) -> dict:
    row = {}
    for idx, header in enumerate(headers):
        key = str(header or "").strip() or f"Column {idx + 1}"
        row[key] = values[idx] if idx < len(values) else ""
    return row


def _extract_tables_from_pdf(path: str) -> tuple[str, list[dict]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise RuntimeError(
            "pdfplumber is required to read pending-verification PDFs. "
            "Install it with: pip install pdfplumber"
        ) from exc

    full_text_parts = []
    rows = []
    word_rows = []
    printed_on = None
    header_columns = None
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            layout_text = ""
            try:
                layout_text = page.extract_text(layout=True) or ""
            except Exception:
                layout_text = ""
            full_text_parts.append(page_text)
            if layout_text:
                full_text_parts.append(layout_text)
            page_word_sets = []
            try:
                page_word_sets.append(page.extract_words() or [])
            except Exception:
                page_word_sets.append([])
            try:
                page_word_sets.append(
                    page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
                )
            except Exception:
                pass
            if printed_on is None:
                for word_set in page_word_sets:
                    printed_on = extract_printed_on_from_words(word_set)
                    if printed_on:
                        break
            if printed_on is None:
                printed_on = extract_printed_on_date(page_text) or extract_printed_on_date(
                    layout_text
                )
            page_columns = None
            for word_set in page_word_sets:
                found_cols = _find_patient_entered_columns(word_set)
                if found_cols:
                    page_columns = found_cols
                    header_columns = found_cols
                    break
            if header_columns:
                extracted = []
                for word_set in page_word_sets:
                    extracted = _extract_patient_entered_from_words(
                        word_set,
                        header_columns,
                        skip_header=page_columns is not None,
                    )
                    if extracted:
                        break
                word_rows.extend(extracted)
            for table in _iter_pdf_tables(page):
                if not table:
                    continue
                body_start, headers = _find_patient_entered_header(table)
                if headers is None:
                    continue
                for raw in table[body_start:]:
                    if not raw or not any(str(c or "").strip() for c in raw):
                        continue
                    rows.append(_row_from_table(headers, raw))
    if not printed_on and not rows and not word_rows:
        pymu_text, pymu_rows, pymu_printed = _extract_with_pymupdf(path)
        if pymu_printed or pymu_rows:
            return pymu_text, rows, pymu_printed, pymu_rows
        if _tesseract_ready():
            ocr_text, ocr_rows, ocr_printed = _ocr_pdf_pages(path)
            return ocr_text or "\n".join(full_text_parts), rows, ocr_printed, ocr_rows
    return "\n".join(full_text_parts), rows, printed_on, word_rows


ALERTS_NAME_RE = re.compile(
    r"([A-Za-z][A-Za-z.'\-]{1,}),\s*([A-Za-z][A-Za-z.'\-]{1,})"
)
ALERTS_DATE_START_RE = re.compile(
    r"^\s*(" + DATE_PATTERNS[0] + r")"
)


def _extract_alerts_report_rows(text: str) -> list[dict]:
    """Parse Alerts Report OCR/text lines: '<Entered> Last, First ...'."""
    rows = []
    lines = [line.rstrip() for line in (text or "").splitlines()]
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        date_match = ALERTS_DATE_START_RE.match(_normalize_pdf_text(line))
        if not date_match:
            idx += 1
            continue
        entered = parse_flexible_date(date_match.group(1))
        rest = line[date_match.end() :]
        rest = re.sub(r"^[\s_\-—–]+", "", rest)
        name_match = ALERTS_NAME_RE.search(rest)
        last = name_match.group(1).strip() if name_match else ""
        first = name_match.group(2).strip() if name_match else ""
        if first.upper() in {"N", "NA", "N/A"}:
            first = ""
        if (not first or "," not in rest) and idx + 1 < len(lines):
            nxt = lines[idx + 1].strip()
            if (
                nxt
                and not ALERTS_DATE_START_RE.match(_normalize_pdf_text(nxt))
                and not nxt.lower().startswith("page")
                and not nxt.lower().startswith("alerts")
            ):
                first_token = re.sub(r"[^A-Za-z.'\-].*$", "", nxt.split()[0])
                if first_token and first_token[0].isalpha() and len(first_token) > 1:
                    if not last:
                        last = re.sub(r"[^A-Za-z.'\-].*$", "", rest.split(",")[0] if "," in rest else rest.split()[0] if rest.split() else "")
                    first = first_token
                    idx += 1
        if entered and last and first:
            rows.append(
                {
                    "Patient": f"{last}, {first}",
                    "Entered": format_display_date(entered),
                }
            )
        idx += 1
    return rows


def _extract_text_rows(text: str) -> list[dict]:
    """Fallback when tables fail: slice Patient / Entered from a header line."""
    lines = [line.rstrip() for line in (text or "").splitlines()]
    header_idx = None
    patient_at = None
    entered_at = None
    for idx, line in enumerate(lines):
        low = line.lower()
        patient_match = re.search(r"\bpatient\b", low)
        entered_match = re.search(r"\bentered\b(?!\s+by)", low)
        if not patient_match or not entered_match:
            continue
        patient_at = patient_match.start()
        entered_at = entered_match.start()
        if patient_at >= 0 and entered_at >= 0:
            header_idx = idx
            break
    if header_idx is None:
        return []
    rows = []
    left, right = sorted((patient_at, entered_at))
    mid = (left + right) // 2
    for line in lines[header_idx + 1 :]:
        raw = line.rstrip()
        if not raw.strip() or PRINTED_LABEL_RE.search(raw):
            continue
        if patient_at < entered_at:
            patient = raw[: max(entered_at, mid)].strip()
            entered = raw[max(entered_at, mid) :].strip()
        else:
            entered = raw[: max(patient_at, mid)].strip()
            patient = raw[max(patient_at, mid) :].strip()
        entered_match = DATE_TOKEN_RE.search(entered)
        if not entered_match:
            continue
        patient = re.sub(r"\s+", " ", patient).strip(" |-:,")
        if len(patient) < 2:
            continue
        rows.append(
            {
                "Patient": patient,
                "Entered": entered_match.group(0),
            }
        )
    return rows


def _pick_patient_and_date(row: dict) -> tuple[str, Any]:
    patient = ""
    row_date = None
    has_entered = any(_is_entered_header(key) for key in row)
    has_patient = any(_is_patient_header(key) for key in row)
    for key, value in row.items():
        if has_patient and _is_patient_header(key) and not patient:
            patient = str(value or "").strip()
        if has_entered and _is_entered_header(key) and row_date is None:
            row_date = parse_flexible_date(value)
    if not patient:
        for key, value in row.items():
            if _looks_like_name_header(key):
                patient = str(value or "").strip()
                break
    if row_date is None and not has_entered:
        for key, value in row.items():
            if _looks_like_date_header(key):
                row_date = parse_flexible_date(value)
                if row_date is not None:
                    break
    return patient, row_date


def collect_all_patient_rows(rows: Iterable[dict]) -> list[dict]:
    """Keep every PDF Patient + Entered row so Excel can match any date."""
    kept = []
    seen = set()
    for row in rows or []:
        patient, row_date = _pick_patient_and_date(row)
        if not patient:
            continue
        key = (normalize_name(patient), format_display_date(row_date))
        if key in seen:
            continue
        seen.add(key)
        kept.append(
            {
                "patient_name": patient,
                "row_date": format_display_date(row_date),
                "raw": dict(row),
            }
        )
    return kept


def filter_previous_date_rows(rows: Iterable[dict], printed_on) -> list[dict]:
    """Keep only rows dated the calendar day before Printed On."""
    target = previous_calendar_date(printed_on)
    kept = []
    for row in collect_all_patient_rows(rows):
        parsed = parse_flexible_date(row.get("row_date"))
        if parsed is None or parsed != target:
            continue
        kept.append(row)
    return kept


def process_pdf_file(path: str, original_filename: str) -> dict:
    doctor_name = extract_doctor_name_from_filename(original_filename)
    text, table_rows, printed_from_words, word_rows = _extract_tables_from_pdf(path)
    printed_on = extract_printed_on_date(text) or printed_from_words
    text_rows = _extract_text_rows(text)
    alerts_rows = _extract_alerts_report_rows(text)

    def _usable_count(candidate):
        count = 0
        for row in candidate or []:
            patient, row_date = _pick_patient_and_date(row)
            if patient and row_date is not None:
                count += 1
        return count

    if alerts_rows:
        source_rows = alerts_rows
    else:
        source_rows = max(
            (table_rows, word_rows, text_rows),
            key=_usable_count,
        )
    patients = collect_all_patient_rows(source_rows)
    previous_patients = filter_previous_date_rows(source_rows, printed_on)
    return {
        "filename": os.path.basename(original_filename),
        "doctor_name": doctor_name,
        "printed_on": format_display_date(printed_on),
        "previous_date": format_display_date(previous_calendar_date(printed_on)),
        "extracted_row_count": len(source_rows),
        "patients": patients,
        "patient_count": len(patients),
        "previous_day_count": len(previous_patients),
        "error": None
        if doctor_name and printed_on is not None
        else (
            "Could not find doctor name in filename"
            if not doctor_name
            else (
                "This PDF has no selectable text (Microsoft Print to PDF outlines). "
                "Install Tesseract OCR so Printed On / Patient / Entered can be read."
                if not (text or "").strip() and not _tesseract_ready()
                else "Could not find Printed On date in PDF"
            )
        ),
    }


def find_column(df: pd.DataFrame, *needles: str):
    if df is None or not hasattr(df, "columns"):
        return None
    wanted = [n.lower() for n in needles]
    for col in df.columns:
        text = str(col).strip().lower()
        if all(n in text for n in wanted):
            return col
    return None


def has_match_columns(df: pd.DataFrame | None) -> bool:
    """True when a sheet has Office, Patient, and Agent columns."""
    if df is None or not hasattr(df, "columns"):
        return False
    office_col = find_column(df, "office", "name") or find_column(df, "office")
    patient_col = find_column(df, "patient", "name") or find_column(df, "patient")
    agent_col = find_column(df, "agent", "name") or find_column(df, "agent")
    return bool(office_col and patient_col and agent_col)


def find_email_id_column(df: pd.DataFrame | None):
    """Prefer 'Email id', then any Email column."""
    if df is None or not hasattr(df, "columns"):
        return None
    email_fallback = None
    for col in df.columns:
        text = str(col).strip().lower()
        if "email" in text and "id" in text:
            return col
        if email_fallback is None and "email" in text:
            email_fallback = col
    return email_fallback


def has_agent_email_columns(df: pd.DataFrame | None) -> bool:
    if df is None or not hasattr(df, "columns"):
        return False
    agent_col = find_column(df, "agent", "name") or find_column(df, "agent")
    return bool(agent_col and find_email_id_column(df))


def _unique_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique = []
    for header in headers:
        key = header or "column"
        count = seen.get(key, 0)
        seen[key] = count + 1
        unique.append(key if count == 0 else f"{key}_{count}")
    return unique


def promote_header_if_needed(df: pd.DataFrame | None, max_scan: int = 25) -> pd.DataFrame | None:
    """If the real header is below title rows, promote that row to columns."""
    if df is None or not hasattr(df, "columns"):
        return df
    if has_match_columns(df):
        return df
    for index in range(min(max_scan, len(df))):
        headers = []
        for value in df.iloc[index].values:
            if pd.isna(value):
                headers.append("")
            else:
                headers.append(str(value).strip())
        if not any(headers):
            continue
        body = df.iloc[index + 1 :].copy()
        if body.empty:
            continue
        body.columns = _unique_headers(headers)
        body.reset_index(drop=True, inplace=True)
        if has_match_columns(body):
            return body
    return df


def promote_agent_header_if_needed(
    df: pd.DataFrame | None, max_scan: int = 25
) -> pd.DataFrame | None:
    """If Agent Name / Email id sit below title rows, promote that header."""
    if df is None or not hasattr(df, "columns"):
        return df
    if has_agent_email_columns(df):
        return df
    for index in range(min(max_scan, len(df))):
        headers = []
        for value in df.iloc[index].values:
            if pd.isna(value):
                headers.append("")
            else:
                headers.append(str(value).strip())
        if not any(headers):
            continue
        body = df.iloc[index + 1 :].copy()
        if body.empty:
            continue
        body.columns = _unique_headers(headers)
        body.reset_index(drop=True, inplace=True)
        if has_agent_email_columns(body):
            return body
    return df


def load_agent_database(path: str) -> list[dict]:
    """Read Agent Name + Email id from every usable sheet."""
    sheets = pd.read_excel(path, sheet_name=None, parse_dates=False)
    staff = []
    seen = set()
    for _name, df in (sheets or {}).items():
        if df is None or getattr(df, "empty", True):
            continue
        promoted = promote_agent_header_if_needed(df)
        agent_col = find_column(promoted, "agent", "name") or find_column(
            promoted, "agent"
        )
        email_col = find_email_id_column(promoted)
        if not agent_col or not email_col:
            continue
        for _, raw in promoted.iterrows():
            agent = raw.get(agent_col)
            email = raw.get(email_col)
            if pd.isna(agent) or pd.isna(email):
                continue
            agent_name = str(agent).strip()
            email_id = str(email).strip()
            if not agent_name or not email_id or "@" not in email_id:
                continue
            key = (normalize_name(agent_name), email_id.lower())
            if key in seen:
                continue
            seen.add(key)
            staff.append({"agent_name": agent_name, "email_id": email_id})
    return staff


def lookup_agent_email(staff_list: list[dict] | None, agent_name: str) -> str | None:
    """Match a table agent name to an Agent Database email."""
    for staff in staff_list or []:
        if names_match(staff.get("agent_name"), agent_name):
            email = str(staff.get("email_id") or "").strip()
            if email:
                return email
    return None


def attach_agent_emails(agents: list[dict] | None, staff_list: list[dict] | None) -> list[dict]:
    attached = []
    for agent in agents or []:
        item = dict(agent)
        item["email_id"] = lookup_agent_email(staff_list, agent.get("agent_name"))
        attached.append(item)
    return attached


def load_pending_excel_workbook(path: str) -> dict[str, pd.DataFrame]:
    """Read every sheet and lift buried headers so patient rows can be found."""
    sheets = pd.read_excel(path, sheet_name=None, parse_dates=False)
    loaded = {}
    for name, df in (sheets or {}).items():
        if df is None or getattr(df, "empty", True):
            continue
        loaded[str(name)] = promote_header_if_needed(df)
    return loaded


def _normalize_excel_source(source) -> list[tuple[str, pd.DataFrame]]:
    if source is None:
        return []
    if isinstance(source, dict):
        return [
            (str(name), df)
            for name, df in source.items()
            if df is not None
        ]
    if isinstance(source, pd.DataFrame):
        return [("", source)]
    if isinstance(source, (list, tuple)):
        items = []
        for index, item in enumerate(source):
            if isinstance(item, tuple) and len(item) == 2:
                items.append((str(item[0]), item[1]))
            elif item is not None:
                items.append((str(index), item))
        return items
    return []


def _iter_excel_rows(
    df: pd.DataFrame,
    source_label: str,
    sheet_name: str = "",
) -> list[dict]:
    if df is None or getattr(df, "empty", True):
        return []
    office_col = find_column(df, "office", "name") or find_column(df, "office")
    patient_col = find_column(df, "patient", "name") or find_column(df, "patient")
    agent_col = find_column(df, "agent", "name") or find_column(df, "agent")
    if not office_col or not patient_col or not agent_col:
        return []
    rows = []
    for _, raw in df.iterrows():
        office = raw.get(office_col)
        patient = raw.get(patient_col)
        agent = raw.get(agent_col)
        if pd.isna(office) or pd.isna(patient) or pd.isna(agent):
            continue
        office = str(office).strip()
        patient = str(patient).strip()
        agent = str(agent).strip()
        if not office or not patient or not agent:
            continue
        record = {str(col): raw[col] for col in df.columns}
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = ""
            elif hasattr(value, "strftime"):
                record[key] = value.strftime("%m/%d/%Y")
            else:
                record[key] = str(value)
        record["Source File"] = source_label
        if sheet_name:
            record["Source Sheet"] = sheet_name
        record["_office_name"] = office
        record["_patient_name"] = patient
        record["_agent_name"] = agent
        rows.append(record)
    return rows


def iter_excel_match_rows(source, source_label: str) -> list[dict]:
    """Collect Office/Patient/Agent rows from every usable sheet."""
    rows = []
    for sheet_name, df in _normalize_excel_source(source):
        promoted = promote_header_if_needed(df)
        rows.extend(_iter_excel_rows(promoted, source_label, sheet_name=sheet_name))
    return rows


def classify_pending_excel_label(filename: str) -> str:
    """Label an Excel upload from its filename when possible."""
    text = str(filename or "").lower()
    if "consolidat" in text:
        return "Consolidate"
    if "allocat" in text:
        return "Allocation Report"
    base = os.path.basename(str(filename or "")).strip()
    return base or "Excel"


def match_pending_rows(
    datasets: list[dict],
    allocation_df: pd.DataFrame | dict | None = None,
    consolidate_df: pd.DataFrame | dict | None = None,
    excel_sources: list[dict] | None = None,
) -> tuple[list[dict], dict]:
    """Match Excel Office Name + Patient Name to PDF doctor datasets; group by agent."""
    doctor_patients = {}
    for dataset in datasets or []:
        doctor = str(dataset.get("doctor_name") or "").strip()
        if not doctor:
            continue
        doctor_patients.setdefault(doctor, [])
        for patient in dataset.get("patients") or []:
            doctor_patients[doctor].append(patient)

    excel_rows = []
    if excel_sources:
        for source in excel_sources:
            label = (
                source.get("label")
                or source.get("filename")
                or "Excel"
            )
            excel_rows.extend(iter_excel_match_rows(source.get("sheets"), label))
    else:
        excel_rows.extend(iter_excel_match_rows(allocation_df, "Allocation Report"))
        excel_rows.extend(iter_excel_match_rows(consolidate_df, "Consolidate"))

    matched_by_agent = {}
    seen = set()
    for row in excel_rows:
        office = row["_office_name"]
        patient = row["_patient_name"]
        agent = row["_agent_name"]
        matched_doctor = None
        for doctor in doctor_patients:
            if names_match(office, doctor):
                matched_doctor = doctor
                break
        if not matched_doctor:
            continue
        patient_hit = any(
            names_match(patient, item.get("patient_name"))
            for item in doctor_patients[matched_doctor]
        )
        if not patient_hit:
            continue
        dedupe_key = (
            normalize_name(agent),
            normalize_name(office),
            normalize_name(patient),
            row.get("Source File"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        display = {
            key: value
            for key, value in row.items()
            if not str(key).startswith("_")
        }
        display["Matched Doctor"] = matched_doctor
        matched_by_agent.setdefault(agent, []).append(display)

    agents = []
    for agent_name in sorted(matched_by_agent, key=str.lower):
        agents.append(
            {
                "agent_name": agent_name,
                "row_count": len(matched_by_agent[agent_name]),
            }
        )
    return agents, matched_by_agent
