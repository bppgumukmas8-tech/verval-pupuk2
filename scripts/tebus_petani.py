#!/usr/bin/env python3
"""
tebus_petani.py - VERSION WITH DEBUG
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
OUTPUT_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1BmaYGnBTAyW6JoI0NGweO0lDgNxiTwH-SiNXTrhRLnM/edit"

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

# =====================================================
# UTIL - TAMBAH LOGGING DETAIL
# =====================================================
def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")

def debug_dataframe(df, name):
    """Debug function untuk melihat info dataframe"""
    log(f"DataFrame: {name}", "DEBUG")
    log(f"  Shape: {df.shape}", "DEBUG")
    log(f"  Columns: {list(df.columns)}", "DEBUG")
    if len(df) > 0:
        log(f"  First NIK: {df['NIK'].iloc[0] if 'NIK' in df.columns else 'N/A'}", "DEBUG")
        log(f"  Last NIK: {df['NIK'].iloc[-1] if 'NIK' in df.columns else 'N/A'}", "DEBUG")

# =====================================================
# MAIN DENGAN DEBUGGING
# =====================================================
def main():
    log("=== SISTEM PEMANTAUAN PENEBUSAN PUPUK (DEBUG MODE) ===")
    
    try:
        # 1. Inisialisasi
        log("1. Inisialisasi Google Drive & Sheets...")
        drive = init_drive()
        gc = init_gspread()
        
        # 2. Load data ERDKK
        log("2. Memuat data ERDKK...")
        erdkk = load_erdkk(drive)
        if erdkk.empty:
            log("❌ Data ERDKK KOSONG!", "ERROR")
            return
        
        debug_dataframe(erdkk, "ERDKK")
        
        # 3. Load data Realisasi
        log("3. Memuat data Realisasi...")
        realisasi, latest_input = load_realisasi(drive)
        if realisasi.empty:
            log("⚠️ Data Realisasi KOSONG!", "WARNING")
        
        debug_dataframe(realisasi, "Realisasi")
        log(f"Tanggal input terakhir: {latest_input}", "INFO")
        
        # 4. Identifikasi yang belum menebus
        log("4. Mencari NIK yang belum menebus...")
        realisasi_nik_set = set(realisasi["NIK"].dropna().unique())
        log(f"Jumlah NIK unik realisasi: {len(realisasi_nik_set)}", "INFO")
        
        # Filter NIK yang belum ada di realisasi
        mask = ~erdkk["NIK"].isin(realisasi_nik_set)
        belum = erdkk[mask].copy()
        
        log(f"Jumlah yang belum menebus: {len(belum)}", "INFO")
        
        if len(belum) == 0:
            log("✅ SEMUA PETANI SUDAH MENEBUS!", "SUCCESS")
        else:
            log(f"⚠️ Masih ada {len(belum)} petani yang belum menebus", "WARNING")
        
        # ... [lanjutkan dengan proses spreadsheet seperti sebelumnya]
        
        # Kirim email dengan data aktual
        total_erdkk_nik = erdkk["NIK"].nunique()
        total_realisasi_nik = realisasi["NIK"].nunique()
        total_belum_nik = belum["NIK"].nunique()
        
        log(f"Summary - ERDKK: {total_erdkk_nik}, Realisasi: {total_realisasi_nik}, Belum: {total_belum_nik}")
        
        send_email_notification(
            total_erdkk_nik,
            total_realisasi_nik,
            total_belum_nik
        )
        
        log("✅ PROSES SELESAI", "SUCCESS")
        
    except Exception as e:
        error_msg = f"{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        log(f"❌ ERROR: {error_msg}", "ERROR")
        
        # Kirim email error
        try:
            cfg = load_email_config()
            subject = "[ERROR CRITICAL] Sistem Pemantauan Pupuk GAGAL"
            body = f"""
SYSTEM ERROR - Proses pemantauan penebusan pupuk GAGAL!

Error Time: {datetime.now()}
Error Details:
{error_msg}

Harap segera periksa server!
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
            log("📧 Email error terkirim", "INFO")
        except Exception as email_error:
            log(f"Gagal kirim email error: {email_error}", "ERROR")
        
        raise

if __name__ == "__main__":
    main()
