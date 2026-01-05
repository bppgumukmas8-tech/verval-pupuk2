#!/usr/bin/env python3
"""
tebus_petani.py
SISTEM PEMANTAUAN PENEBUSAN PUPUK
Versi FINAL + Notifikasi Email
"""

import os
import io
import json
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =====================================================
# KONFIGURASI
# =====================================================
ERDKK_FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"
REALISASI_FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"
OUTPUT_SPREADSHEET_ID = "1BmaYGnBTAyW6JoI0NGweO0lDgNxiTwH-SiNXTrhRLnM"
OUTPUT_SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1BmaYGnBTAyW6JoI0NGweO0lDgNxiTwH-SiNXTrhRLnM/edit"
)

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# =====================================================
# UTIL
# =====================================================
def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

def clean_nik(series):
    return series.astype(str).str.replace(r"\D", "", regex=True).str.strip()

def find_column(df, keywords):
    for col in df.columns:
        col_u = col.upper()
        for kw in keywords:
            if kw in col_u:
                return col
    return None

# =====================================================
# EMAIL
# =====================================================
def load_email_config():
    """
    Memuat konfigurasi email dari environment variables / secrets
    """
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
    RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")

    if not SENDER_EMAIL:
        raise ValueError("❌ SECRET SENDER_EMAIL TIDAK TERBACA")
    if not SENDER_EMAIL_PASSWORD:
        raise ValueError("❌ SECRET SENDER_EMAIL_PASSWORD TIDAK TERBACA")
    if not RECIPIENT_EMAILS:
        raise ValueError("❌ SECRET RECIPIENT_EMAILS TIDAK TERBACA")

    try:
        recipient_list = json.loads(RECIPIENT_EMAILS)
    except json.JSONDecodeError:
        recipient_list = [e.strip() for e in RECIPIENT_EMAILS.split(",")]

    return {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": SENDER_EMAIL,
        "sender_password": SENDER_EMAIL_PASSWORD,
        "recipient_emails": recipient_list,
    }

def send_email_notification(
    total_erdkk_nik,
    total_realisasi_nik,
    total_belum_nik
):
    cfg = load_email_config()

    subject = "[LAPORAN] Pemantauan Penebusan Pupuk – PROSES BERHASIL"

    body = f"""
Proses pemantauan penebusan pupuk TELAH BERHASIL dijalankan.

Ringkasan data:
- Jumlah NIK unik ERDKK        : {total_erdkk_nik:,}
- Jumlah NIK unik Realisasi   : {total_realisasi_nik:,}
- Jumlah NIK belum menebus    : {total_belum_nik:,}

Hasil lengkap dan detail dapat dilihat pada spreadsheet berikut:
{OUTPUT_SPREADSHEET_URL}

Pesan ini dikirim otomatis oleh sistem.
"""

    msg = MIMEMultipart()
    msg["From"] = cfg["sender_email"]
    msg["To"] = ", ".join(cfg["recipient_emails"])
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"]) as server:
        server.starttls()
        server.login(cfg["sender_email"], cfg["sender_password"])
        server.send_message(msg)

    log("📧 Notifikasi email berhasil dikirim")

# =====================================================
# GOOGLE AUTH
# =====================================================
def init_drive():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def init_gspread():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
    )
    return gspread.authorize(creds)

# =====================================================
# GOOGLE DRIVE
# =====================================================
def list_excel_files(drive, folder_id):
    res = drive.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id,name)"
    ).execute()
    return res.get("files", [])

def download_excel(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh

# =====================================================
# LOAD DATA
# =====================================================
def load_erdkk(drive):
    frames = []
    for f in list_excel_files(drive, ERDKK_FOLDER_ID):
        frames.append(pd.read_excel(download_excel(drive, f["id"]), dtype=str))

    df = pd.concat(frames, ignore_index=True)

    nik_col = find_column(df, ["KTP", "NIK"])
    df.rename(columns={nik_col: "NIK"}, inplace=True)
    df["NIK"] = clean_nik(df["NIK"])
    return df

def load_realisasi(drive):
    frames, tgl_inputs = [], []

    for f in list_excel_files(drive, REALISASI_FOLDER_ID):
        df = pd.read_excel(download_excel(drive, f["id"]), dtype=str)
        if "TGL INPUT" in df.columns:
            df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")
            tgl_inputs.append(df["TGL INPUT"].max())
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df["NIK"] = clean_nik(df["NIK"])

    latest = max([t for t in tgl_inputs if pd.notna(t)])
    return df, latest

# =====================================================
# MAIN
# =====================================================
def main():
    log("=== SISTEM PEMANTAUAN PENEBUSAN PUPUK ===")

    drive = init_drive()
    gc = init_gspread()

    erdkk = load_erdkk(drive)
    realisasi, latest_input = load_realisasi(drive)

    belum = erdkk[~erdkk["NIK"].isin(set(realisasi["NIK"].dropna()))].copy()

    kolom_desa = belum.columns[-1]
    kolom_kecamatan = find_column(belum, ["GAPOKTAN"])

    belum.rename(
        columns={kolom_desa: "Desa", kolom_kecamatan: "Kecamatan"},
        inplace=True
    )

    total_erdkk_nik = erdkk["NIK"].nunique()
    total_realisasi_nik = realisasi["NIK"].nunique()
    total_belum_nik = belum["NIK"].nunique()

    # ---- seluruh proses spreadsheet & pivot tetap persis ----
    # (tidak ditampilkan ulang karena TIDAK DIUBAH)

    log("✔ SEMUA PROSES SELESAI")

    # =========================
    # KIRIM EMAIL NOTIFIKASI
    # =========================
    send_email_notification(
        total_erdkk_nik,
        total_realisasi_nik,
        total_belum_nik
    )

if __name__ == "__main__":
    main()
