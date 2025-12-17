"""
data_tebus_versi_web.py
Script untuk cleaning dan reordering data Verval Pupuk untuk versi web.
Proses: membersihkan NIK dan mengubah urutan kolom.

Lokasi: verval-pupuk2/scripts/data_tebus_versi_web.py
"""

import os
import sys
import pandas as pd
import gspread
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import traceback
import json
import io

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"  # Folder Google Drive

# GUNAKAN SPREADSHEET YANG SAMA DENGAN WORKFLOW LAIN
SPREADSHEET_ID = "10Do2tgCtdJadFcMF0pYOWdgz9c8s3HrWXxOQMIzRW3Y"
SHEET_NAME = "Data_Gabungan"  # Nama sheet untuk hasil

# ============================
# LOAD EMAIL CONFIGURATION FROM SECRETS
# ============================
def load_email_config():
    """
    Memuat konfigurasi email dari environment variables/secrets
    """
    # Load dari environment variables
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
    RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")
    
    # Validasi
    if not SENDER_EMAIL:
        raise ValueError("❌ SECRET SENDER_EMAIL TIDAK TERBACA")
    if not SENDER_EMAIL_PASSWORD:
        raise ValueError("❌ SECRET SENDER_EMAIL_PASSWORD TIDAK TERBACA")
    if not RECIPIENT_EMAILS:
        raise ValueError("❌ SECRET RECIPIENT_EMAILS TIDAK TERBACA")
    
    # Parse recipient emails
    try:
        # Coba parse sebagai JSON array
        recipient_list = json.loads(RECIPIENT_EMAILS)
    except json.JSONDecodeError:
        # Jika bukan JSON, split berdasarkan koma
        recipient_list = [email.strip() for email in RECIPIENT_EMAILS.split(",")]
    
    return {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": SENDER_EMAIL,
        "sender_password": SENDER_EMAIL_PASSWORD,
        "recipient_emails": recipient_list
    }

# ============================
# FUNGSI BERSIHKAN NIK
# ============================
def clean_nik(nik_value):
    """
    Membersihkan NIK dari karakter non-angka seperti ', `, spasi, dll.
    Hanya mengambil angka saja.
    """
    if pd.isna(nik_value) or nik_value is None:
        return None
    
    # Convert ke string dan hilangkan semua karakter non-digit
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)  # \D = non-digit
    
    # Validasi panjang NIK (biasanya 16 digit)
    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")
    
    return cleaned_nik if cleaned_nik else None

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """
    Mengirim notifikasi email tentang status proses
    """
    try:
        # Load config email
        EMAIL_CONFIG = load_email_config()
        
        # Konfigurasi email
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = f"[verval-pupuk2] {subject}"
        
        # Style untuk email
        if is_success:
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: green;">✅ {subject}</h2>
                    <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                        {message.replace(chr(10), '<br>')}
                    </div>
                    <p><small>📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py</small></p>
                    <p><small>⏰ Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
                </body>
            </html>
            """
        else:
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: red;">❌ {subject}</h2>
                    <div style="background-color: #ffe6e6; padding: 15px; border-radius: 5px;">
                        {message.replace(chr(10), '<br>')}
                    </div>
                    <p><small>📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py</small></p>
                    <p><small>⏰ Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
                </body>
            </html>
            """
        
        msg.attach(MIMEText(email_body, 'html'))
        
        # Kirim email
        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_CONFIG["sender_email"], EMAIL_CONFIG["sender_password"])
            server.send_message(msg)
        
        print(f"📧 Notifikasi email terkirim ke {len(EMAIL_CONFIG['recipient_emails'])} penerima")
        return True
        
    except Exception as e:
        print(f"❌ Gagal mengirim email: {str(e)}")
        return False

# ============================
# FUNGSI DOWNLOAD FILE
# ============================
def download_excel_files(folder_id, save_folder="data_web"):
    """
    Download file Excel dari Google Drive
    """
    os.makedirs(save_folder, exist_ok=True)
    
    # Load credentials
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan")
    
    credentials = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # Query untuk mencari file Excel
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ Tidak ada file Excel di folder Google Drive.")

    paths = []
    for f in files:
        print(f"📥 Downloading: {f['name']}")
        request = drive_service.files().get_media(fileId=f["id"])
        file_path = os.path.join(save_folder, f["name"])
        
        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        
        paths.append({
            'path': file_path,
            'name': f['name']
        })
    
    print(f"✅ Berhasil download {len(paths)} file Excel")
    return paths

# ============================
# FUNGSI UTAMA
# ============================
def process_data_for_web():
    """
    Fungsi utama untuk processing data versi web
    """
    print("=" * 60)
    print("🚀 PROSES CLEANING & REORDERING DATA UNTUK WEB")
    print("=" * 60)
    print(f"📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py")
    print(f"📂 Folder ID: {FOLDER_ID}")
    print(f"📊 Spreadsheet ID: {SPREADSHEET_ID}")
    print(f"📄 Sheet Name: {SHEET_NAME}")
    print("=" * 60)
    
    try:
        log = []
        all_data = []
        total_rows = 0
        file_count = 0
        nik_cleaning_log = []

        print("🔍 Memulai proses cleaning dan reordering data...")
        
        # ========== LOAD CREDENTIALS ==========
        creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("❌ SECRET GOOGLE_APPLICATION_CREDENTIALS_JSON TIDAK TERBACA")

        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )

        gc = gspread.authorize(credentials)
        
        # Download semua Excel
        excel_files = download_excel_files(FOLDER_ID, save_folder="data_web")
        print(f"📁 Berhasil download {len(excel_files)} file Excel")

        # Proses setiap file Excel
        for file_info in excel_files:
            file_count += 1
            fpath = file_info['path']
            filename = file_info['name']
            
            print(f"\n📖 Memproses: {filename}")
            
            try:
                df = pd.read_excel(fpath, dtype=str)  # pastikan NIK terbaca full string
                
                # PROSES BERSIHKAN NIK
                original_nik_count = len(df)
                df['NIK_ORIGINAL'] = df['NIK']  # Simpan nilai asli untuk logging
                df['NIK'] = df['NIK'].apply(clean_nik)
                
                # Log NIK yang dibersihkan
                cleaned_niks = df[df['NIK_ORIGINAL'] != df['NIK']][['NIK_ORIGINAL', 'NIK']]
                for _, row in cleaned_niks.iterrows():
                    nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")
                
                # Hapus baris dengan NIK kosong setelah cleaning
                df = df[df['NIK'].notna()]
                cleaned_nik_count = len(df)
                
                total_rows += cleaned_nik_count
                log.append(f"- {filename}: {original_nik_count} -> {cleaned_nik_count} baris (setelah cleaning NIK)")
                
                # Pastikan kolom pupuk bertipe numeric
                pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']
                for col in pupuk_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                all_data.append(df)
                print(f"   ✅ Berhasil: {cleaned_nik_count} baris")
                
            except Exception as e:
                print(f"   ❌ Error memproses {filename}: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            print(f"❌ ERROR: {error_msg}")
            send_email_notification("CLEANING DATA WEB GAGAL", error_msg, is_success=False)
            return False

        # Gabungkan semua data
        combined = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan: {len(combined):,} baris")

        # Pastikan kolom sesuai header
        original_columns = [
            'KECAMATAN', 'NO TRANSAKSI', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
            'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
            'TGL TEBUS', 'STATUS'
        ]
        
        # Handle jika ada kolom yang missing
        for col in original_columns:
            if col not in combined.columns:
                combined[col] = ""
        
        combined = combined[original_columns]

        # REORDER KOLOM sesuai permintaan
        new_column_order = [
            'NIK',           # (1) - dari (4)
            'NAMA PETANI',   # (2) - dari (5)  
            'KECAMATAN',     # (3) - dari (1)
            'NAMA KIOS',     # (4) - dari (3)
            'NO TRANSAKSI',  # (5) - dari (2)
            'UREA',          # (6) - tetap (6)
            'NPK',           # (7) - tetap (7)
            'SP36',          # (8) - tetap (8)
            'ZA',            # (9) - tetap (9)
            'NPK FORMULA',   # (10) - tetap (10)
            'ORGANIK',       # (11) - tetap (11)
            'ORGANIK CAIR',  # (12) - tetap (12)
            'TGL TEBUS',     # (13) - tetap (13)
            'STATUS'         # (14) - tetap (14)
        ]
        
        # Apply reordering
        combined_df = combined[new_column_order]

        # Tulis ke Google Sheet
        print(f"\n📤 Mengupload data ke Google Sheets...")
        print(f"   Spreadsheet: {SPREADSHEET_ID}")
        print(f"   Sheet: {SHEET_NAME}")
        
        try:
            sh = gc.open_by_key(SPREADSHEET_ID)
            
            # Cek apakah sheet sudah ada
            try:
                ws = sh.worksheet(SHEET_NAME)
                print(f"   ✅ Sheet '{SHEET_NAME}' ditemukan, membersihkan...")
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                # Buat sheet baru jika tidak ada
                print(f"   📄 Sheet '{SHEET_NAME}' tidak ditemukan, membuat baru...")
                ws = sh.add_worksheet(SHEET_NAME, rows=1, cols=len(new_column_order))
            
            # Upload data
            set_with_dataframe(ws, combined_df)
            print(f"   ✅ Data berhasil diupload: {len(combined_df):,} baris × {len(combined_df.columns)} kolom")
            
        except Exception as e:
            print(f"   ❌ Gagal mengupload ke Google Sheets: {str(e)}")
            raise

        # Buat summary untuk email
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
CLEANING & REORDERING DATA UNTUK WEB BERHASIL ✓

📊 STATISTIK UMUM:
• Repository: verval-pupuk2/scripts/data_tebus_versi_web.py
• Tanggal Proses: {now}
• File Diproses: {file_count}
• Total Data: {total_rows:,} baris
• Unique NIK: {combined_df['NIK'].nunique():,}
• NIK Dibersihkan: {len(nik_cleaning_log):,} entri

🔄 PERUBAHAN URUTAN KOLOM:
1. NIK (1) ← dari (4)
2. NAMA PETANI (2) ← dari (5)  
3. KECAMATAN (3) ← dari (1)
4. NAMA KIOS (4) ← dari (3)
5. NO TRANSAKSI (5) ← dari (2)
6. UREA hingga STATUS (6-14) ← tetap

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN:
{chr(10).join(nik_cleaning_log[:10])}
{"... (masih ada yang lain)" if len(nik_cleaning_log) > 10 else ""}

✅ Data telah berhasil diupload ke Google Sheets:
• Spreadsheet: {SPREADSHEET_ID}
• Sheet: {SHEET_NAME}
• URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid=0

🎯 FITUR:
✅ Cleaning NIK otomatis (hapus karakter non-digit)
✅ Validasi panjang NIK (16 digit)
✅ Konversi kolom pupuk ke numerik
✅ Reordering kolom untuk kebutuhan web
✅ Upload otomatis ke Google Sheets
✅ Notifikasi email lengkap
"""

        # Print ke console
        print(f"\n✅ Cleaning & Reordering selesai!")
        print(f"   ⏰ Waktu: {now}")
        print(f"   📁 File: {file_count}")
        print(f"   📊 Baris: {total_rows:,}")
        print(f"   👥 Unique NIK: {combined_df['NIK'].nunique():,}")
        print(f"   🔧 NIK Dibersihkan: {len(nik_cleaning_log):,}")
        
        # Kirim email notifikasi sukses
        send_email_notification("CLEANING DATA WEB BERHASIL", success_message, is_success=True)
        
        print("\n" + "=" * 60)
        print("✅ PROSES SELESAI DENGAN SUKSES!")
        print("=" * 60)
        
        return True

    except Exception as e:
        # Buat error message
        error_message = f"""
CLEANING DATA UNTUK WEB GAGAL ❌

📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py
📅 Tanggal Proses: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
⚠️ Error: {str(e)}

🔧 Traceback:
{traceback.format_exc()}
"""
        print("\n❌ CLEANING DATA GAGAL")
        print(f"❌ {str(e)}")
        
        # Kirim email notifikasi error
        send_email_notification("CLEANING DATA WEB GAGAL", error_message, is_success=False)
        
        return False

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_data_for_web()
