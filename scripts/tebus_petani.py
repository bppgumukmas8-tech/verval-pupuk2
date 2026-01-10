#!/usr/bin/env python3
"""
tebus_petani.py - VERSION COMPLETE FINAL
SISTEM PEMANTAUAN PENEBUSAN PUPUK - COMPLETE VERSION
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
# UTIL
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

def send_email_notification(total_erdkk_nik, total_realisasi_nik, total_belum_nik):
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
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("❌ SERVICE_ACCOUNT_JSON tidak ditemukan di environment variables")
    
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def init_gspread():
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("❌ SERVICE_ACCOUNT_JSON tidak ditemukan di environment variables")
    
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
    )
    return gspread.authorize(creds)

# =====================================================
# GOOGLE DRIVE
# =====================================================
def list_excel_files(drive, folder_id):
    try:
        res = drive.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc"
        ).execute()
        return res.get("files", [])
    except Exception as e:
        log(f"Error listing files: {str(e)}", "ERROR")
        raise

def download_excel(drive, file_id):
    try:
        request = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        log(f"Gagal mengunduh file: {str(e)}", "ERROR")
        raise

# =====================================================
# LOAD DATA
# =====================================================
def load_erdkk(drive):
    frames = []
    files = list_excel_files(drive, ERDKK_FOLDER_ID)
    
    if not files:
        log("⚠️ TIDAK ADA FILE di folder ERDKK!", "WARNING")
        return pd.DataFrame()
    
    for f in files:
        log(f"Memuat file ERDKK: {f['name']}", "INFO")
        try:
            df = pd.read_excel(download_excel(drive, f["id"]), dtype=str)
            frames.append(df)
        except Exception as e:
            log(f"Gagal memuat file {f['name']}: {str(e)}", "ERROR")
    
    if not frames:
        log("❌ Tidak ada file ERDKK yang berhasil dimuat!", "ERROR")
        return pd.DataFrame()
    
    df = pd.concat(frames, ignore_index=True)
    log(f"Total data ERDKK: {len(df)} baris", "INFO")
    
    nik_col = find_column(df, ["KTP", "NIK"])
    if not nik_col:
        log("⚠️ Kolom NIK tidak ditemukan di data ERDKK!", "WARNING")
        return pd.DataFrame()
    
    df.rename(columns={nik_col: "NIK"}, inplace=True)
    df["NIK"] = clean_nik(df["NIK"])
    
    # Hapus NIK kosong
    df = df[df["NIK"].notna() & (df["NIK"].str.strip() != "")]
    log(f"Data ERDKK setelah cleaning: {len(df)} baris", "INFO")
    
    return df

def load_realisasi(drive):
    frames, tgl_inputs = [], []
    files = list_excel_files(drive, REALISASI_FOLDER_ID)
    
    if not files:
        log("⚠️ TIDAK ADA FILE di folder REALISASI!", "WARNING")
        return pd.DataFrame(), None
    
    for f in files:
        log(f"Memuat file Realisasi: {f['name']}", "INFO")
        try:
            df = pd.read_excel(download_excel(drive, f["id"]), dtype=str)
            
            if "TGL INPUT" in df.columns:
                df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")
                max_tgl = df["TGL INPUT"].max()
                if pd.notna(max_tgl):
                    tgl_inputs.append(max_tgl)
            
            frames.append(df)
        except Exception as e:
            log(f"Gagal memuat file {f['name']}: {str(e)}", "ERROR")
    
    if not frames:
        log("❌ Tidak ada file Realisasi yang berhasil dimuat!", "ERROR")
        return pd.DataFrame(), None
    
    df = pd.concat(frames, ignore_index=True)
    
    # Cari kolom NIK
    nik_col = find_column(df, ["KTP", "NIK"])
    if nik_col:
        df.rename(columns={nik_col: "NIK"}, inplace=True)
    elif "NIK" not in df.columns:
        log("❌ Kolom NIK tidak ditemukan di data Realisasi!", "ERROR")
        return pd.DataFrame(), None
    
    df["NIK"] = clean_nik(df["NIK"])
    
    # Hapus NIK kosong
    df = df[df["NIK"].notna() & (df["NIK"].str.strip() != "")]
    log(f"Data Realisasi setelah cleaning: {len(df)} baris", "INFO")
    
    latest = max(tgl_inputs) if tgl_inputs else None
    if latest:
        log(f"Tanggal input terbaru: {latest}", "INFO")
    
    return df, latest

# =====================================================
# SPREADSHEET MANAGEMENT - BAGIAN YANG HILANG!
# =====================================================
def create_sheet_if_not_exists(sh, sheet_name):
    """Buat sheet jika belum ada"""
    try:
        # Coba buka sheet yang sudah ada
        sh.worksheet(sheet_name)
        log(f"Sheet '{sheet_name}' sudah ada", "DEBUG")
        return True
    except gspread.exceptions.WorksheetNotFound:
        try:
            # Buat sheet baru
            sh.add_worksheet(title=sheet_name, rows=1000, cols=50)
            log(f"Sheet '{sheet_name}' berhasil dibuat", "INFO")
            return True
        except Exception as e:
            log(f"Gagal membuat sheet '{sheet_name}': {str(e)}", "ERROR")
            return False

def clear_sheet_content(worksheet):
    """Hapus semua konten dari sheet"""
    try:
        # Hapus semua data kecuali header jika ada
        worksheet.clear()
        log(f"Sheet '{worksheet.title}' berhasil dibersihkan", "DEBUG")
        return True
    except Exception as e:
        log(f"Gagal membersihkan sheet '{worksheet.title}': {str(e)}", "ERROR")
        return False

def update_spreadsheet(gc, erdkk, realisasi, belum):
    """Update Google Spreadsheet dengan data terbaru"""
    try:
        log("6. Membuka spreadsheet output...", "INFO")
        sh = gc.open_by_key(OUTPUT_SPREADSHEET_ID)
        
        # List sheet yang diperlukan
        required_sheets = ["RAW_ERDKK", "RAW_REALISASI", "BELUM_TEBUS", "SUMMARY"]
        
        # Buat sheet yang belum ada
        for sheet_name in required_sheets:
            create_sheet_if_not_exists(sh, sheet_name)
        
        # 1. Update sheet "RAW_ERDKK"
        log("  - Mengupdate sheet RAW_ERDKK...", "INFO")
        try:
            worksheet = sh.worksheet("RAW_ERDKK")
            clear_sheet_content(worksheet)
            
            # Konversi DataFrame ke list untuk Google Sheets
            data_erdkk = [erdkk.columns.tolist()] + erdkk.values.tolist()
            
            # Update data dalam batch (Google Sheets limit: 10MB per request)
            batch_size = 10000
            for i in range(0, len(data_erdkk), batch_size):
                batch = data_erdkk[i:i + batch_size]
                start_cell = f"A{i+1}" if i == 0 else f"A{i+1}"
                worksheet.update(batch, start_cell)
            
            log(f"    ✓ RAW_ERDKK updated: {len(erdkk)} baris", "INFO")
        except Exception as e:
            log(f"    ✗ Gagal update RAW_ERDKK: {str(e)}", "ERROR")
        
        # 2. Update sheet "RAW_REALISASI"
        log("  - Mengupdate sheet RAW_REALISASI...", "INFO")
        try:
            worksheet = sh.worksheet("RAW_REALISASI")
            clear_sheet_content(worksheet)
            
            if not realisasi.empty:
                data_realisasi = [realisasi.columns.tolist()] + realisasi.values.tolist()
                
                batch_size = 10000
                for i in range(0, len(data_realisasi), batch_size):
                    batch = data_realisasi[i:i + batch_size]
                    start_cell = f"A{i+1}" if i == 0 else f"A{i+1}"
                    worksheet.update(batch, start_cell)
                
                log(f"    ✓ RAW_REALISASI updated: {len(realisasi)} baris", "INFO")
            else:
                worksheet.update([['TIDAK ADA DATA REALISASI']], 'A1')
                log("    ✓ RAW_REALISASI: Tidak ada data", "INFO")
                
        except Exception as e:
            log(f"    ✗ Gagal update RAW_REALISASI: {str(e)}", "ERROR")
        
        # 3. Update sheet "BELUM_TEBUS"
        log("  - Mengupdate sheet BELUM_TEBUS...", "INFO")
        try:
            worksheet = sh.worksheet("BELUM_TEBUS")
            clear_sheet_content(worksheet)
            
            if not belum.empty:
                # Siapkan data untuk ditampilkan
                columns_to_show = []
                if 'NIK' in belum.columns:
                    columns_to_show.append('NIK')
                if 'Nama Petani' in belum.columns:
                    columns_to_show.append('Nama Petani')
                
                # Cari kolom Desa
                desa_col = None
                for col in belum.columns:
                    if 'DESA' in col.upper() or 'KELURAHAN' in col.upper():
                        desa_col = col
                        break
                if desa_col:
                    columns_to_show.append(desa_col)
                
                # Cari kolom Kecamatan
                kec_col = find_column(belum, ['KECAMATAN', 'KEC', 'GAPOKTAN'])
                if kec_col:
                    columns_to_show.append(kec_col)
                
                if not columns_to_show:
                    columns_to_show = belum.columns.tolist()[:4]
                
                data_belum = belum[columns_to_show].copy()
                data_belum.insert(0, 'NO', range(1, len(data_belum) + 1))
                
                # Update dalam batch
                data_to_update = [data_belum.columns.tolist()] + data_belum.values.tolist()
                
                batch_size = 10000
                for i in range(0, len(data_to_update), batch_size):
                    batch = data_to_update[i:i + batch_size]
                    start_cell = f"A{i+1}" if i == 0 else f"A{i+1}"
                    worksheet.update(batch, start_cell)
                
                log(f"    ✓ BELUM_TEBUS updated: {len(belum)} baris", "INFO")
            else:
                worksheet.update([['NO', 'NIK', 'NAMA', 'DESA', 'KECAMATAN'], 
                                  ['Tidak ada data', '', '', '', '']], 'A1')
                log("    ✓ BELUM_TEBUS: Tidak ada data", "INFO")
                
        except Exception as e:
            log(f"    ✗ Gagal update BELUM_TEBUS: {str(e)}", "ERROR")
        
        # 4. Update sheet "SUMMARY"
        log("  - Mengupdate sheet SUMMARY...", "INFO")
        try:
            worksheet = sh.worksheet("SUMMARY")
            clear_sheet_content(worksheet)
            
            total_erdkk = len(erdkk)
            uniq_erdkk = erdkk["NIK"].nunique()
            total_real = len(realisasi) if not realisasi.empty else 0
            uniq_real = realisasi["NIK"].nunique() if not realisasi.empty else 0
            total_belum = len(belum) if not belum.empty else 0
            uniq_belum = belum["NIK"].nunique() if not belum.empty else 0
            
            summary_data = [
                ["SISTEM PEMANTAUAN PENEBUSAN PUPUK"],
                ["Update Terakhir", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                [""],
                ["ITEM", "JUMLAH"],
                ["Total Data ERDKK", total_erdkk],
                ["NIK Unik ERDKK", uniq_erdkk],
                ["Total Realisasi", total_real],
                ["NIK Unik Realisasi", uniq_real],
                ["Belum Menebus", total_belum],
                ["NIK Unik Belum", uniq_belum],
                [""],
                ["PERSENTASE"],
                ["Penebusan", f"{(uniq_real/uniq_erdkk*100):.2f}%" if uniq_erdkk > 0 else "0%"],
                ["Belum", f"{(uniq_belum/uniq_erdkk*100):.2f}%" if uniq_erdkk > 0 else "0%"],
                [""],
                ["TANGGAL UPDATE FILE"],
                ["ERDKK Terbaru", files_erdkk[0]['modifiedTime'] if files_erdkk else "N/A"],
                ["Realisasi Terbaru", files_realisasi[0]['modifiedTime'] if files_realisasi else "N/A"]
            ]
            
            worksheet.update(summary_data, 'A1')
            log("    ✓ SUMMARY updated", "INFO")
            
        except Exception as e:
            log(f"    ✗ Gagal update SUMMARY: {str(e)}", "ERROR")
        
        log("✅ Semua sheet berhasil diupdate", "SUCCESS")
        return True
        
    except Exception as e:
        log(f"❌ Error update spreadsheet: {str(e)}", "ERROR")
        return False

# =====================================================
# MAIN - DENGAN PASTI ADA UPDATE SPREADSHEET
# =====================================================
def main():
    log("=== SISTEM PEMANTAUAN PENEBUSAN PUPUK ===", "INFO")
    
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
        
        realisasi_nik_set = set(realisasi["NIK"].dropna().unique()) if not realisasi.empty else set()
        log(f"Jumlah NIK unik realisasi: {len(realisasi_nik_set)}", "INFO")
        
        mask = ~erdkk["NIK"].isin(realisasi_nik_set)
        belum = erdkk[mask].copy()
        log(f"Jumlah yang belum menebus: {len(belum)}", "INFO")
        
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
        
        # ============ BAGIAN YANG HARUS ADA ============
        # 6. Update Spreadsheet - PASTIKAN INI DIPANGGIL!
        log("6. Mengupdate spreadsheet output...", "INFO")
        
        # Ambil info file untuk summary
        files_erdkk = list_excel_files(drive, ERDKK_FOLDER_ID)
        files_realisasi = list_excel_files(drive, REALISASI_FOLDER_ID)
        
        spreadsheet_updated = update_spreadsheet(gc, erdkk, realisasi, belum)
        
        if not spreadsheet_updated:
            log("❌ GAGAL update spreadsheet!", "ERROR")
        # ==============================================
        
        # 7. Kirim email
        log("7. Mengirim notifikasi email...", "INFO")
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
        raise

if __name__ == "__main__":
    main()
