import os
import io
import json
import pandas as pd
import gspread
import re
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"
SAVE_FOLDER = "data_erdkk"
SPREADSHEET_ID = "1aEx7cgw1KIdpXo20dD3LnCHF6PWer1wWgT7H5YKSqlY"
SHEET_NAME = "Hasil_Rekap"

# ============================
# LOAD CREDENTIALS
# ============================
creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_json:
    raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan")

credentials = Credentials.from_service_account_info(
    json.loads(creds_json),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc = gspread.authorize(credentials)
drive_service = build("drive", "v3", credentials=credentials)

# ============================
# FUNGSI WRITE KE GOOGLE SHEETS (FIXED)
# ============================
def write_to_google_sheet(worksheet, data_rows):
    """
    Menulis data ke Google Sheets dengan chunking
    (VERSI FIX: TANPA LIMIT 200.000 BARIS)
    """
    print(f"📤 Menulis {len(data_rows)} baris ke Google Sheets")

    total_rows = len(data_rows)
    total_cols = len(data_rows[0])

    total_cells = total_rows * total_cols
    print(f"📊 Ukuran data: {total_rows} x {total_cols} = {total_cells:,} cells")

    # 🔧 FIX: Validasi limit resmi Google Sheets
    MAX_CELLS = 10_000_000
    if total_cells > MAX_CELLS:
        raise ValueError(
            f"❌ Data melebihi limit Google Sheets "
            f"({total_cells:,} > 10,000,000 cells)"
        )

    # Clear worksheet
    worksheet.clear()
    time.sleep(2)

    # Resize worksheet (tanpa batas 200.000)
    buffer_rows = 100
    buffer_cols = 5
    new_rows = total_rows + buffer_rows
    new_cols = total_cols + buffer_cols

    worksheet.resize(rows=new_rows, cols=new_cols)
    print(f"🔄 Worksheet di-resize ke {new_rows} x {new_cols}")

    # Chunking
    CHUNK_SIZE = 5000  # lebih stabil untuk data besar
    for start in range(0, total_rows, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, total_rows)
        chunk = data_rows[start:end]
        cell = f"A{start + 1}"

        print(f"   ✍️ Menulis baris {start + 1}–{end}")
        worksheet.update(
            cell,
            chunk,
            value_input_option="RAW"  # lebih cepat & stabil
        )
        time.sleep(1)

    print("✅ Semua data berhasil ditulis")
    return True

# ============================
# CONTOH MAIN (LOGIKA TETAP)
# ============================
def main():
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=30)

    # ====== CONTOH DATA (GANTI DENGAN HASIL PIVOT ANDA) ======
    header = [f"COL_{i}" for i in range(1, 24)]
    data = [header]

    for i in range(203250):
        data.append([f"data_{i}_{j}" for j in range(23)])

    write_to_google_sheet(ws, data)

if __name__ == "__main__":
    main()
