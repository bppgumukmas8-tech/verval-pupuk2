#!/usr/bin/env python3
"""
tebus_petani.py
SISTEM PEMANTAUAN PENEBUSAN PUPUK
FINAL VERSION + DEBUG LOGGING + EMAIL NOTIFICATION
"""

import os
import io
import json
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

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
# UTIL + DEBUG
# =====================================================
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")

def clean_nik(series):
    return series.astype(str).str.replace(r"\D", "", regex=True).str.strip()

def find_column(df, keywords):
    for col in df.columns:
        col_u = col.upper()
        for kw in keywords:
            if kw in col_u:
                return col
    return None

def debug_df(df, name):
    log(f"DF {name} | shape={df.shape}", "DEBUG")
    log(f"DF {name} | columns={list(df.columns)}", "DEBUG")
    if "NIK" in df.columns:
        log(
            f"DF {name} | NIK count={df['NIK'].count()} "
            f"unique={df['NIK'].nunique()}",
            "DEBUG",
        )

# =====================================================
# EMAIL
# =====================================================
def load_email_config():
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
        recipients = json.loads(RECIPIENT_EMAILS)
    except json.JSONDecodeError:
        recipients = [e.strip() for e in RECIPIENT_EMAILS.split(",")]

    return {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": SENDER_EMAIL,
        "sender_password": SENDER_EMAIL_PASSWORD,
        "recipient_emails": recipients,
    }

def send_email_notification(total_erdkk_nik, total_realisasi_nik, total_belum_nik):
    cfg = load_email_config()

    subject = "[LAPORAN] Pemantauan Penebusan Pupuk – PROSES BERHASIL"

    body = f"""
Proses pemantauan penebusan pupuk TELAH BERHASIL dijalankan.

Ringkasan:
- NIK unik ERDKK      : {total_erdkk_nik:,}
- NIK unik Realisasi : {total_realisasi_nik:,}
- Belum menebus      : {total_belum_nik:,}

Detail lengkap:
{OUTPUT_SPREADSHEET_URL}

Email ini dikirim otomatis oleh sistem.
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

    log("📧 Email notifikasi terkirim", "INFO")

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
        q=f"'{folder_id}' in parents "
        f"and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id,name)",
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
        df = pd.read_excel(download_excel(drive, f["id"]), dtype=str)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)

    nik_col = find_column(df, ["KTP", "NIK"])
    df.rename(columns={nik_col: "NIK"}, inplace=True)
    df["NIK"] = clean_nik(df["NIK"])

    debug_df(df, "ERDKK")
    return df

def load_realisasi(drive):
    frames, tgl_inputs = [], []

    for f in list_excel_files(drive, REALISASI_FOLDER_ID):
        df = pd.read_excel(download_excel(drive, f["id"]), dtype=str)

        if "TGL INPUT" in df.columns:
            df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")
            tgl_inputs.append(df["TGL INPUT"].max())

        nik_col = find_column(df, ["KTP", "NIK"])
        if nik_col:
            df.rename(columns={nik_col: "NIK"}, inplace=True)

        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df["NIK"] = clean_nik(df["NIK"])

    debug_df(df, "REALISASI")

    latest = max([t for t in tgl_inputs if pd.notna(t)])
    return df, latest

# =====================================================
# MAIN
# =====================================================
def main():
    log("=== SISTEM PEMANTAUAN PENEBUSAN PUPUK ===", "INFO")

    try:
        drive = init_drive()
        gc = init_gspread()

        erdkk = load_erdkk(drive)
        realisasi, latest_input = load_realisasi(drive)

        belum = erdkk[~erdkk["NIK"].isin(set(realisasi["NIK"].dropna()))].copy()

        kolom_desa = belum.columns[-1]
        kolom_kecamatan = find_column(belum, ["GAPOKTAN"])

        belum.rename(
            columns={kolom_desa: "Desa", kolom_kecamatan: "Kecamatan"},
            inplace=True,
        )

        # =========================
        # STATISTIK
        # =========================
        total_erdkk_rows = len(erdkk)
        total_erdkk_nik = erdkk["NIK"].nunique()
        total_realisasi_rows = len(realisasi)
        total_realisasi_nik = realisasi["NIK"].nunique()
        total_belum_nik = belum["NIK"].nunique()

        # =========================
        # SPREADSHEET
        # =========================
        sh = gc.open_by_key(OUTPUT_SPREADSHEET_ID)

        try:
            ws_info = sh.worksheet("Sheet1")
        except gspread.WorksheetNotFound:
            ws_info = sh.add_worksheet("Sheet1", 100, 20)

        for ws in sh.worksheets():
            if ws.title != "Sheet1":
                sh.del_worksheet(ws)

        ws_info.clear()
        ws_info.update(
            "A1:B9",
            [
                ["Update Tanggal", latest_input.strftime("%d %B %Y")],
                ["Update Jam", latest_input.strftime("%H:%M:%S")],
                ["", ""],
                ["Jumlah baris ERDKK", total_erdkk_rows],
                ["Jumlah NIK unik ERDKK", total_erdkk_nik],
                ["Jumlah baris Realisasi", total_realisasi_rows],
                ["Jumlah NIK unik Realisasi", total_realisasi_nik],
                ["Jumlah petani belum tebus", total_belum_nik],
            ],
        )

        # =========================
        # DATA BELUM TEBUS
        # =========================
        data_petani = belum[
            [
                "Kecamatan",
                "Desa",
                "Nama Petani",
                "NIK",
                "Kode Kios Pengecer",
                "Nama Kios Pengecer",
            ]
        ]

        ws_data = sh.add_worksheet(
            "Data Petani Belum Tebus",
            min(len(data_petani) + 10, 1_000_000),
            len(data_petani.columns),
        )

        ws_data.update("A1", [data_petani.columns.tolist()])
        for i in range(0, len(data_petani), 10000):
            ws_data.update(
                f"A{i+2}",
                data_petani.iloc[i : i + 10000]
                .fillna("")
                .astype(str)
                .values.tolist(),
            )

        # =========================
        # PIVOT KEC
        # =========================
        pivot_kec = (
            data_petani.groupby("Kecamatan")["NIK"]
            .nunique()
            .reset_index(name="Jumlah Petani")
        )
        pivot_kec.loc[len(pivot_kec)] = ["TOTAL", total_belum_nik]

        ws_kec = sh.add_worksheet("pivot_kec", len(pivot_kec) + 5, 2)
        ws_kec.update("A1", [["Kecamatan", "Jumlah Petani"]] + pivot_kec.values.tolist())

        # =========================
        # PIVOT DESA
        # =========================
        pivot_desa = (
            data_petani.groupby(["Kecamatan", "Desa"])["NIK"]
            .nunique()
            .reset_index(name="Jumlah Petani")
        )
        pivot_desa.loc[len(pivot_desa)] = ["TOTAL", "", total_belum_nik]

        ws_desa = sh.add_worksheet("pivot_desa", len(pivot_desa) + 5, 3)
        ws_desa.update(
            "A1",
            [["Kecamatan", "Desa", "Jumlah Petani"]] + pivot_desa.values.tolist(),
        )

        # =========================
        # PIVOT KIOS
        # =========================
        pivot_kios = (
            data_petani.groupby(
                [
                    "Kecamatan",
                    "Desa",
                    "Kode Kios Pengecer",
                    "Nama Kios Pengecer",
                ]
            )["NIK"]
            .nunique()
            .reset_index(name="Jumlah Petani")
        )
        pivot_kios.loc[len(pivot_kios)] = ["TOTAL", "", "", "", total_belum_nik]

        ws_kios = sh.add_worksheet("pivot_kios", len(pivot_kios) + 5, 5)
        ws_kios.update(
            "A1",
            [
                [
                    "Kecamatan",
                    "Desa",
                    "Kode Kios",
                    "Nama Kios",
                    "Jumlah Petani",
                ]
            ]
            + pivot_kios.values.tolist(),
        )

        send_email_notification(
            total_erdkk_nik,
            total_realisasi_nik,
            total_belum_nik,
        )

        log("✔ SEMUA PROSES SELESAI", "SUCCESS")

    except Exception as e:
        log(str(e), "ERROR")
        log(traceback.format_exc(), "ERROR")
        raise

if __name__ == "__main__":
    main()
