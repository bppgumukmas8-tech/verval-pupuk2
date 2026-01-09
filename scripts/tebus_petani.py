#!/usr/bin/env python3
"""
tebus_petani.py - VERSION WITH DEBUG
SISTEM PEMANTAUAN PENEBUSAN PUPUK - DEBUG VERSION
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

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# =====================================================
# UTIL - TAMBAH LOGGING DETAIL
# =====================================================
def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")

def clean_nik(series):
    return series.astype(str).str.replace(r"\D", "", regex=True).str.strip()

def find_column(df, keywords):
    for col in df.columns:
        col_u = col.upper()
        for kw in keywords:
            if kw in col_u:
                return col
    return None

def debug_dataframe(df, name):
    """Debug function untuk melihat info dataframe"""
    log(f"DataFrame: {name}", "DEBUG")
    log(f"  Shape: {df.shape}", "DEBUG")
    log(f"  Columns: {list(df.columns)}", "DEBUG")
    if len(df) > 0:
        log(f"  First NIK: {df['NIK'].iloc[0] if 'NIK' in df.columns else 'N/A'}", "DEBUG")
        log(f"  Last NIK: {df['NIK'].iloc[-1] if 'NIK' in df.columns else 'N/A'}", "DEBUG")
    if 'NIK' in df.columns:
        log(f"  NIK count: {df['NIK'].count()}, NIK unique: {df['NIK'].nunique()}", "DEBUG")

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

    log("📧 Notifikasi email berhasil dikirim", "INFO")

# =====================================================
# GOOGLE AUTH
# =====================================================
def init_drive():
    log("Menginisialisasi Google Drive API...", "DEBUG")
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("❌ SERVICE_ACCOUNT_JSON tidak ditemukan di environment variables")
    
    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
        )
        drive_service = build("drive", "v3", credentials=creds)
        log("Google Drive API berhasil diinisialisasi", "DEBUG")
        return drive_service
    except Exception as e:
        log(f"Gagal menginisialisasi Google Drive: {str(e)}", "ERROR")
        raise

def init_gspread():
    log("Menginisialisasi Google Sheets API...", "DEBUG")
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("❌ SERVICE_ACCOUNT_JSON tidak ditemukan di environment variables")
    
    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
        )
        gc = gspread.authorize(creds)
        log("Google Sheets API berhasil diinisialisasi", "DEBUG")
        return gc
    except Exception as e:
        log(f"Gagal menginisialisasi Google Sheets: {str(e)}", "ERROR")
        raise

# =====================================================
# GOOGLE DRIVE
# =====================================================
def list_excel_files(drive, folder_id, folder_name=""):
    """List semua file Excel di folder dengan debug info"""
    try:
        log(f"Mencari file Excel di folder {folder_name or folder_id}...", "DEBUG")
        res = drive.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
            fields="files(id,name,createdTime,modifiedTime)",
            orderBy="modifiedTime desc"
        ).execute()
        
        files = res.get("files", [])
        log(f"Ditemukan {len(files)} file Excel di folder {folder_name or folder_id}", "INFO")
        
        # Debug: print info file
        for i, f in enumerate(files[:5]):  # Tampilkan 5 file terbaru
            mod_time = datetime.strptime(f['modifiedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
            log(f"  {i+1}. {f['name']} (mod: {mod_time.strftime('%Y-%m-%d %H:%M')})", "DEBUG")
        
        if len(files) > 5:
            log(f"  ... dan {len(files)-5} file lainnya", "DEBUG")
            
        return files
    except Exception as e:
        log(f"Error listing files di folder {folder_id}: {str(e)}", "ERROR")
        raise

def download_excel(drive, file_id, file_name=""):
    try:
        log(f"Mengunduh file: {file_name or file_id}", "DEBUG")
        request = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        log(f"File berhasil diunduh: {file_name or file_id}", "DEBUG")
        return fh
    except Exception as e:
        log(f"Gagal mengunduh file {file_id}: {str(e)}", "ERROR")
        raise

# =====================================================
# LOAD DATA
# =====================================================
def load_erdkk(drive):
    log("=== MEMUAT DATA ERDKK ===", "INFO")
    frames = []
    
    files = list_excel_files(drive, ERDKK_FOLDER_ID, "ERDKK")
    
    if not files:
        log("⚠️ TIDAK ADA FILE di folder ERDKK!", "WARNING")
        return pd.DataFrame()
    
    for i, f in enumerate(files, 1):
        log(f"Memuat file ERDKK {i}/{len(files)}: {f['name']}", "INFO")
        try:
            df = pd.read_excel(download_excel(drive, f["id"], f["name"]), dtype=str)
            log(f"  - Berhasil: {len(df)} baris, {len(df.columns)} kolom", "DEBUG")
            frames.append(df)
        except Exception as e:
            log(f"  - Gagal memuat file {f['name']}: {str(e)}", "ERROR")
    
    if not frames:
        log("❌ Tidak ada file ERDKK yang berhasil dimuat!", "ERROR")
        return pd.DataFrame()
    
    df = pd.concat(frames, ignore_index=True)
    log(f"Total data ERDKK setelah concat: {len(df)} baris", "INFO")
    
    # Cari kolom NIK
    nik_col = find_column(df, ["KTP", "NIK"])
    if not nik_col:
        log("⚠️ Kolom NIK tidak ditemukan di data ERDKK!", "WARNING")
        log(f"Kolom yang tersedia: {list(df.columns)}", "DEBUG")
        # Coba kolom pertama yang mengandung angka (mungkin NIK)
        for col in df.columns:
            sample = df[col].astype(str).str.strip().iloc[0] if len(df) > 0 else ""
            if sample.isdigit() and len(sample) >= 10:
                nik_col = col
                log(f"Menggunakan kolom {col} sebagai NIK (deteksi otomatis)", "INFO")
                break
    
    if nik_col:
        df.rename(columns={nik_col: "NIK"}, inplace=True)
        df["NIK"] = clean_nik(df["NIK"])
        log(f"Menggunakan kolom '{nik_col}' sebagai NIK", "INFO")
    else:
        log("❌ Tidak bisa menemukan kolom NIK!", "ERROR")
        return pd.DataFrame()
    
    debug_dataframe(df, "ERDKK (setelah cleaning)")
    
    # Hapus NIK kosong
    before = len(df)
    df = df[df["NIK"].notna() & (df["NIK"].str.strip() != "")]
    after = len(df)
    log(f"Menghapus NIK kosong: {before} → {after} baris", "INFO")
    
    return df

def load_realisasi(drive):
    log("=== MEMUAT DATA REALISASI ===", "INFO")
    frames, tgl_inputs = [], []

    files = list_excel_files(drive, REALISASI_FOLDER_ID, "REALISASI")
    
    if not files:
        log("⚠️ TIDAK ADA FILE di folder REALISASI!", "WARNING")
        return pd.DataFrame(), None
    
    for i, f in enumerate(files, 1):
        log(f"Memuat file Realisasi {i}/{len(files)}: {f['name']}", "INFO")
        try:
            df = pd.read_excel(download_excel(drive, f["id"], f["name"]), dtype=str)
            log(f"  - Berhasil: {len(df)} baris, {len(df.columns)} kolom", "DEBUG")
            
            # Cari kolom TGL INPUT
            if "TGL INPUT" in df.columns:
                try:
                    df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")
                    max_tgl = df["TGL INPUT"].max()
                    if pd.notna(max_tgl):
                        tgl_inputs.append(max_tgl)
                        log(f"  - TGL INPUT terbaru: {max_tgl}", "DEBUG")
                except Exception as e:
                    log(f"  - Error parsing TGL INPUT: {str(e)}", "WARNING")
            
            # Cari kolom NIK
            nik_col = find_column(df, ["KTP", "NIK"])
            if nik_col:
                df.rename(columns={nik_col: "NIK"}, inplace=True)
                log(f"  - Menggunakan kolom '{nik_col}' sebagai NIK", "DEBUG")
            elif "NIK" not in df.columns:
                log("  - ⚠️ Kolom NIK tidak ditemukan di file ini", "WARNING")
                continue
            
            frames.append(df)
        except Exception as e:
            log(f"  - Gagal memuat file {f['name']}: {str(e)}", "ERROR")
    
    if not frames:
        log("❌ Tidak ada file Realisasi yang berhasil dimuat!", "ERROR")
        return pd.DataFrame(), None
    
    df = pd.concat(frames, ignore_index=True)
    
    # Pastikan kolom NIK ada
    if "NIK" not in df.columns:
        log("❌ Kolom NIK tidak ditemukan di data Realisasi!", "ERROR")
        return pd.DataFrame(), None
    
    df["NIK"] = clean_nik(df["NIK"])
    
    # Hapus NIK kosong
    before = len(df)
    df = df[df["NIK"].notna() & (df["NIK"].str.strip() != "")]
    after = len(df)
    log(f"Menghapus NIK kosong: {before} → {after} baris", "INFO")
    
    debug_dataframe(df, "Realisasi (setelah cleaning)")
    
    # Cari tanggal input terbaru
    latest = None
    if tgl_inputs:
        latest = max(tgl_inputs)
        log(f"Tanggal input terbaru di Realisasi: {latest}", "INFO")
    
    return df, latest

# =====================================================
# MAIN DENGAN DEBUGGING
# =====================================================
def main():
    log("=== SISTEM PEMANTAUAN PENEBUSAN PUPUK (DEBUG MODE) ===", "INFO")
    
    try:
        # 1. Inisialisasi
        log("1. Inisialisasi Google Drive & Sheets...", "INFO")
        drive = init_drive()
        gc = init_gspread()
        
        # 2. Load data ERDKK
        log("2. Memuat data ERDKK...", "INFO")
        erdkk = load_erdkk(drive)
        if erdkk.empty:
            log("❌ Data ERDKK KOSONG! Script dihentikan.", "ERROR")
            return
        
        # 3. Load data Realisasi
        log("3. Memuat data Realisasi...", "INFO")
        realisasi, latest_input = load_realisasi(drive)
        if realisasi.empty:
            log("⚠️ Data Realisasi KOSONG!", "WARNING")
        
        # 4. Identifikasi yang belum menebus
        log("4. Mencari NIK yang belum menebus...", "INFO")
        
        # Ambil NIK unik dari realisasi
        realisasi_nik = realisasi["NIK"].dropna().unique() if not realisasi.empty else []
        realisasi_nik_set = set(realisasi_nik)
        log(f"Jumlah NIK unik realisasi: {len(realisasi_nik_set)}", "INFO")
        
        # Filter NIK yang belum ada di realisasi
        mask = ~erdkk["NIK"].isin(realisasi_nik_set)
        belum = erdkk[mask].copy()
        
        log(f"Jumlah yang belum menebus: {len(belum)}", "INFO")
        
        if len(belum) == 0:
            log("✅ SEMUA PETANI SUDAH MENEBUS!", "SUCCESS")
        else:
            log(f"⚠️ Masih ada {len(belum)} petani yang belum menebus", "WARNING")
        
        # 5. Hitung summary
        total_erdkk_nik = erdkk["NIK"].nunique()
        total_realisasi_nik = len(realisasi_nik_set)
        total_belum_nik = belum["NIK"].nunique() if not belum.empty else 0
        
        log("=" * 50, "INFO")
        log("SUMMARY DATA:", "INFO")
        log(f"- Jumlah NIK unik ERDKK        : {total_erdkk_nik:,}", "INFO")
        log(f"- Jumlah NIK unik Realisasi   : {total_realisasi_nik:,}", "INFO")
        log(f"- Jumlah NIK belum menebus    : {total_belum_nik:,}", "INFO")
        log("=" * 50, "INFO")
        
        # 6. Kirim email
        log("5. Mengirim notifikasi email...", "INFO")
        try:
            send_email_notification(
                total_erdkk_nik,
                total_realisasi_nik,
                total_belum_nik
            )
        except Exception as email_error:
            log(f"⚠️ Gagal mengirim email: {str(email_error)}", "WARNING")
        
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
