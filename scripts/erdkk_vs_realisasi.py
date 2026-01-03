"""
erdkk_vs_realisasi_fixed_v5.py
Script untuk analisis perbandingan data ERDKK vs Realisasi Penebusan Pupuk.
VERSI DIPERBAIKI - Menambahkan informasi tanggal input terbaru dari data realisasi.

Lokasi: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed_v5.py
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
from datetime import datetime, timedelta
import traceback
import json
import time
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import tempfile
from dateutil import parser

# ============================
# KONFIGURASI
# ============================
ERDKK_FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"  # Folder ERDKK
REALISASI_FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"  # Folder realisasi
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1xiMkISdgcquqt69dbFek8mEc0UNOZmtAALVgX5jaPJc/edit"

# OPTIMIZED RATE LIMITING
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 30
WRITE_DELAY = 5
BATCH_DELAY = 10

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
# FUNGSI EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """Mengirim notifikasi email"""
    try:
        # Load config email
        EMAIL_CONFIG = load_email_config()
        
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = f"[verval-pupuk2] {subject}"

        if is_success:
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: green;">✅ {subject}</h2>
                    <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                        {message.replace(chr(10), '<br>')}
                    </div>
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed_v5.py</small></p>
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed_v5.py</small></p>
                    <p><small>⏰ Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
                </body>
            </html>
            """

        msg.attach(MIMEText(email_body, 'html'))

        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_CONFIG["sender_email"], EMAIL_CONFIG["sender_password"])
            server.send_message(msg)

        print(f"📧 Email terkirim ke {len(EMAIL_CONFIG['recipient_emails'])} penerima")
        return True

    except Exception as e:
        print(f"❌ Gagal mengirim email: {str(e)}")
        return False

# ============================
# FUNGSI BANTU UNTUK FILTER STATUS
# ============================
def is_status_disetujui_pusat(status_value):
    """
    Cek apakah status termasuk kategori 'Disetujui Pusat'
    Kriteria:
    1. Harus mengandung kata 'disetujui' (case insensitive)
    2. Harus mengandung kata 'pusat' (case insensitive)
    3. TIDAK BOLEH mengandung kata 'menunggu' (case insensitive)
    4. TIDAK BOLEH mengandung kata 'ditolak' (case insensitive)
    """
    if pd.isna(status_value) or status_value is None:
        return False
    
    status_str = str(status_value).lower()
    
    # Kriteria 1: Harus mengandung 'disetujui'
    contains_disetujui = 'disetujui' in status_str
    
    # Kriteria 2: Harus mengandung 'pusat'
    contains_pusat = 'pusat' in status_str
    
    # Kriteria 3: Tidak boleh mengandung 'menunggu'
    contains_menunggu = 'menunggu' in status_str
    
    # Kriteria 4: Tidak boleh mengandung 'ditolak'
    contains_ditolak = 'ditolak' in status_str
    
    # Harus memenuhi semua kriteria
    return contains_disetujui and contains_pusat and not contains_menunggu and not contains_ditolak

def print_status_analysis(df, status_column='STATUS'):
    """Analisis dan print semua status yang ada"""
    if status_column not in df.columns:
        print("   ⚠️  Kolom STATUS tidak ditemukan")
        return
    
    status_counts = df[status_column].value_counts()
    total_data = len(df)
    
    print(f"\n   📊 ANALISIS STATUS ({total_data} data):")
    for status, count in status_counts.items():
        percentage = (count / total_data) * 100
        is_disetujui_pusat = is_status_disetujui_pusat(status)
        marker = "✅" if is_disetujui_pusat else "  "
        
        # Tambahkan penjelasan untuk status yang ambigu
        status_lower = str(status).lower()
        contains_disetujui = 'disetujui' in status_lower
        contains_pusat = 'pusat' in status_lower
        contains_menunggu = 'menunggu' in status_lower
        contains_ditolak = 'ditolak' in status_lower
        
        notes = []
        if contains_disetujui and not is_disetujui_pusat:
            if not contains_pusat:
                notes.append("tidak ada 'pusat'")
            if contains_menunggu:
                notes.append("ada 'menunggu'")
            if contains_ditolak:
                notes.append("ada 'ditolak'")
        
        note_str = f" ({', '.join(notes)})" if notes else ""
        
        print(f"      {marker} {status}: {count} data ({percentage:.1f}%){note_str}")

# ============================
# FUNGSI BANTU UNTUK PARSING TANGGAL
# ============================
def parse_tanggal_input(tanggal_value):
    """Parse tanggal input dari berbagai format"""
    if pd.isna(tanggal_value) or tanggal_value is None:
        return None
    
    # Jika sudah datetime, langsung return
    if isinstance(tanggal_value, datetime):
        return tanggal_value
    
    try:
        # Coba parse dengan dateutil
        return parser.parse(str(tanggal_value), dayfirst=True)
    except:
        try:
            # Coba format umum
            formats = [
                '%d-%m-%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%d-%m-%Y %H:%M',
                '%d/%m/%Y %H:%M',
                '%Y-%m-%d',
                '%d-%m-%Y',
                '%d/%m/%Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(str(tanggal_value).strip(), fmt)
                except:
                    continue
            
            # Coba extract tanggal dari string
            tanggal_str = str(tanggal_value)
            # Cari pola tanggal
            date_patterns = [
                r'(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})',
                r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, tanggal_str)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        try:
                            if len(groups[2]) == 4:  # Format YYYY
                                if len(groups[0]) == 4:  # YYYY-MM-DD
                                    return datetime(int(groups[0]), int(groups[1]), int(groups[2]))
                                else:  # DD-MM-YYYY
                                    return datetime(int(groups[2]), int(groups[1]), int(groups[0]))
                            else:  # Format YY
                                if len(groups[0]) == 2:  # DD-MM-YY
                                    year = int(groups[2])
                                    if year < 100:
                                        year += 2000 if year < 50 else 1900
                                    return datetime(year, int(groups[1]), int(groups[0]))
                        except:
                            continue
            
            return None
        except:
            return None

def extract_latest_tanggal_input_from_file(file_path):
    """Ekstrak tanggal input terbaru dari file realisasi"""
    try:
        print(f"   🔍 Mengekstrak tanggal input dari file...")
        
        # Baca file dengan engine openpyxl untuk handle berbagai format
        df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Cari kolom yang mungkin berisi tanggal input
        tanggal_cols = []
        for col in df.columns:
            col_upper = col.upper()
            # Pattern untuk kolom tanggal
            if any(pattern in col_upper for pattern in ['TANGGAL', 'DATE', 'WAKTU', 'TIME', 'INPUT', 'DIBUAT', 'CREATED']):
                tanggal_cols.append(col)
        
        # Jika tidak ditemukan, coba semua kolom
        if not tanggal_cols:
            tanggal_cols = df.columns.tolist()
        
        latest_date = None
        
        for col in tanggal_cols:
            try:
                # Ambil sample untuk analisis
                sample_data = df[col].dropna().head(10).tolist()
                if not sample_data:
                    continue
                
                print(f"   🔍 Cek kolom '{col}': sample = {sample_data[:3]}")
                
                # Coba parse setiap nilai
                for value in df[col].dropna().head(100):  # Cek 100 baris pertama
                    try:
                        parsed_date = parse_tanggal_input(value)
                        if parsed_date:
                            if latest_date is None or parsed_date > latest_date:
                                latest_date = parsed_date
                    except:
                        continue
                        
                if latest_date:
                    print(f"   ✅ Tanggal terbaru dari kolom '{col}': {latest_date}")
                    return latest_date
                    
            except Exception as e:
                print(f"   ⚠️  Error processing column {col}: {e}")
                continue
        
        # Coba cari tanggal dari nama file
        try:
            # Pattern: mencari tanggal dalam format YYYY-MM-DD atau DD-MM-YYYY dalam nama file
            filename = os.path.basename(file_path)
            date_patterns = [
                r'(\d{4})[-_](\d{1,2})[-_](\d{1,2})',  # YYYY-MM-DD
                r'(\d{1,2})[-_](\d{1,2})[-_](\d{4})',  # DD-MM-YYYY
                r'(\d{1,2})[-_](\d{1,2})[-_](\d{2})'   # DD-MM-YY
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, filename)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        if len(groups[0]) == 4:  # YYYY-MM-DD
                            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        elif len(groups[2]) == 4:  # DD-MM-YYYY
                            day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                        else:  # DD-MM-YY
                            day, month, year_str = int(groups[0]), int(groups[1]), groups[2]
                            year = int(year_str)
                            if year < 100:
                                year += 2000 if year < 50 else 1900
                        
                        file_date = datetime(year, month, day)
                        print(f"   📅 Tanggal dari nama file: {file_date}")
                        
                        # Coba cari waktu dari data
                        latest_time = None
                        for col in df.columns:
                            for value in df[col].dropna().head(20):
                                if isinstance(value, str) and ':' in value:
                                    time_match = re.search(r'(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?', value)
                                    if time_match:
                                        hour = int(time_match.group(1))
                                        minute = int(time_match.group(2))
                                        second = int(time_match.group(3)) if time_match.group(3) else 0
                                        latest_time = timedelta(hours=hour, minutes=minute, seconds=second)
                                        break
                            if latest_time:
                                break
                        
                        if latest_time:
                            file_date = datetime.combine(file_date.date(), (datetime.min + latest_time).time())
                        
                        return file_date
        except Exception as e:
            print(f"   ⚠️  Error extracting date from filename: {e}")
        
        return None
        
    except Exception as e:
        print(f"   ❌ Error extracting tanggal input: {e}")
        return None

# ============================
# FUNGSI BANTU UNTUK GOOGLE API
# ============================
def exponential_backoff(attempt):
    base_delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 1))
    jitter = base_delay * 0.1
    return base_delay + jitter

def safe_google_api_operation(operation, *args, **kwargs):
    """Safe operation dengan exponential backoff"""
    last_exception = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = operation(*args, **kwargs)
            if attempt > 1:
                print(f"   ✅ Berhasil pada percobaan ke-{attempt}")
            return result
            
        except HttpError as e:
            last_exception = e
            if e.resp.status == 429:
                if attempt < MAX_RETRIES:
                    wait_time = exponential_backoff(attempt)
                    print(f"⏳ Quota exceeded, menunggu {wait_time:.1f} detik... (Percobaan {attempt}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Gagal setelah {MAX_RETRIES} percobaan")
                    raise e
            elif e.resp.status in [500, 502, 503, 504]:
                if attempt < MAX_RETRIES:
                    wait_time = exponential_backoff(attempt)
                    print(f"⏳ Server error {e.resp.status}, menunggu {wait_time:.1f} detik... (Percobaan {attempt}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                else:
                    raise e
            else:
                raise e
        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                wait_time = exponential_backoff(attempt)
                print(f"⏳ Error {type(e).__name__}, menunggu {wait_time:.1f} detik... (Percobaan {attempt}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                raise e
    
    raise last_exception

def clean_nik(nik_value):
    """Membersihkan NIK dari karakter non-angka"""
    if pd.isna(nik_value) or nik_value is None:
        return None
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)
    if len(cleaned_nik) != 16:
        # Tidak selalu error, bisa jadi data valid dengan leading zeros hilang
        if len(cleaned_nik) < 16:
            # Tambahkan leading zeros
            cleaned_nik = cleaned_nik.zfill(16)
    return cleaned_nik if cleaned_nik else None

def clean_column_name(col_name):
    """Bersihkan nama kolom"""
    if pd.isna(col_name):
        return ""
    col_str = str(col_name)
    col_clean = col_str.strip().upper()
    col_clean = re.sub(r'\s+', ' ', col_clean)
    return col_clean

# ============================
# FUNGSI DOWNLOAD FILE
# ============================
def download_excel_files_from_drive(credentials, folder_id, folder_name):
    """Download file Excel dari Google Drive"""
    print(f"\n📥 Download file dari folder: {folder_name}")
    print(f"   🔍 Folder ID: {folder_id}")
    
    # Buat temporary folder
    temp_dir = tempfile.gettempdir()
    save_folder = os.path.join(temp_dir, f"data_{folder_name}_{int(time.time())}")
    os.makedirs(save_folder, exist_ok=True)
    
    try:
        drive_service = build('drive', 'v3', credentials=credentials)

        # Query untuk mencari file Excel
        query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel' or mimeType='application/vnd.google-apps.spreadsheet')"
        print(f"   🔍 Query: {query}")
        
        results = drive_service.files().list(q=query, fields="files(id, name, mimeType, modifiedTime)").execute()
        files = results.get("files", [])

        if not files:
            print(f"⚠️  Tidak ada file Excel di folder {folder_name}")
            return []

        file_paths = []
        for file in files:
            print(f"   📥 Downloading: {file['name']} ({file['mimeType']})")
            print(f"      📅 Modified: {file.get('modifiedTime', 'N/A')}")
            
            try:
                # Handle Google Sheets vs regular Excel
                if file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                    # Export Google Sheets ke Excel
                    request = drive_service.files().export_media(
                        fileId=file["id"],
                        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                    ext = '.xlsx'
                else:
                    # Regular Excel file
                    request = drive_service.files().get_media(fileId=file["id"])
                    ext = '.xlsx' if file['name'].lower().endswith('.xlsx') else '.xls'
                
                # Gunakan nama file yang aman
                safe_filename = "".join(c for c in file['name'] if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
                if not safe_filename.lower().endswith(('.xlsx', '.xls')):
                    safe_filename += ext
                    
                file_path = os.path.join(save_folder, safe_filename)

                with io.FileIO(file_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            print(f"      Progress: {int(status.progress() * 100)}%")

                file_paths.append({
                    'path': file_path,
                    'name': file['name'],
                    'temp_folder': save_folder,
                    'mime_type': file['mimeType'],
                    'modified_time': file.get('modifiedTime')
                })
                print(f"      ✅ Berhasil download: {safe_filename}")

            except Exception as e:
                print(f"      ❌ Gagal download {file['name']}: {str(e)}")
                continue

        print(f"✅ Berhasil download {len(file_paths)} file Excel dari {folder_name}")
        return file_paths

    except Exception as e:
        print(f"❌ Error download dari {folder_name}: {str(e)}")
        traceback.print_exc()
        return []

# ============================
# FUNGSI PROSES DATA ERDKK
# ============================
def process_erdkk_file(file_path, file_name):
    """Proses satu file ERDKK"""
    try:
        print(f"\n   📖 Memproses ERDKK: {file_name}")

        # Baca file Excel
        df = pd.read_excel(file_path, dtype=str)
        
        # Standardize column names
        df.columns = df.columns.astype(str).str.strip().str.upper()
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada: {list(df.columns)}")
        
        # ============================================
        # IDENTIFIKASI KOLOM UTAMA
        # ============================================
        
        # Cari kolom KTP/NIK
        ktp_cols = [col for col in df.columns if 'KTP' in col or 'NIK' in col]
        if ktp_cols:
            ktp_col = ktp_cols[0]
        else:
            # Cari kolom yang mungkin berisi NIK (16 digit)
            for col in df.columns:
                if len(df) > 0:
                    sample = df[col].head(3).astype(str).tolist()
                    for val in sample:
                        if isinstance(val, str):
                            digits = re.sub(r'\D', '', val)
                            if len(digits) == 16:
                                ktp_col = col
                                break
                    if 'ktp_col' in locals():
                        break
            
            if 'ktp_col' not in locals() and len(df.columns) > 0:
                ktp_col = df.columns[0]
        
        print(f"   🔍 Kolom KTP/NIK: {ktp_col}")
        if len(df) > 0:
            sample_niks = df[ktp_col].head(3).tolist()
            print(f"   🔍 Sample NIK (3 pertama): {sample_niks}")
        
        # Cari kolom Nama Petani
        nama_cols = [col for col in df.columns if 'NAMA' in col and 'PETANI' in col]
        if nama_cols:
            nama_col = nama_cols[0]
        else:
            nama_cols = [col for col in df.columns if 'NAMA' in col]
            if nama_cols:
                nama_col = nama_cols[0]
            else:
                nama_col = ''
        print(f"   🔍 Kolom Nama: {nama_col}")
        
        # Cari kolom Kode Kios
        kode_kios_cols = [col for col in df.columns if 'KODE' in col and 'KIOS' in col]
        if kode_kios_cols:
            kode_kios_col = kode_kios_cols[0]
        else:
            kode_kios_col = ''
        print(f"   🔍 Kolom Kode Kios: {kode_kios_col}")
        
        # Cari kolom Nama Kios
        nama_kios_cols = [col for col in df.columns if 'NAMA' in col and 'KIOS' in col]
        if nama_kios_cols:
            nama_kios_col = nama_kios_cols[0]
        else:
            nama_kios_col = ''
        print(f"   🔍 Kolom Nama Kios: {nama_kios_col}")
        
        # ============================================
        # CARI KOLOM KECAMATAN DARI GAPOKTAN
        # ============================================
        kec_col = ''
        
        # 1. Cari kolom KECAMATAN langsung (prioritas tinggi)
        kec_patterns_primary = ['KECAMATAN', 'KEC', 'WILAYAH KECAMATAN']
        for col in df.columns:
            col_upper = col.upper()
            for pattern in kec_patterns_primary:
                if pattern in col_upper:
                    kec_col = col
                    print(f"   ✅ Kolom Kecamatan ditemukan: {kec_col}")
                    
                    # Tampilkan sample data kecamatan
                    if len(df) > 0:
                        sample_kec = df[kec_col].head(3).tolist()
                        print(f"   🔍 Sample Kecamatan (3 pertama): {sample_kec}")
                    break
            if kec_col:
                break
        
        # 2. Jika tidak ada, cari kolom GAPOKTAN (case insensitive)
        if not kec_col:
            gapoktan_patterns = ['GAPOKTAN', 'GABUNGAN KELOMPOK TANI', 'GAPOKTAN/NAMA KELOMPOK']
            for col in df.columns:
                col_upper = col.upper()
                for pattern in gapoktan_patterns:
                    if pattern in col_upper:
                        kec_col = col
                        print(f"   ✅ Menggunakan kolom GAPOKTAN sebagai Kecamatan: {kec_col}")
                        
                        # Tampilkan sample data GAPOKTAN
                        if len(df) > 0:
                            sample_gapoktan = df[kec_col].head(5).tolist()
                            print(f"   🔍 Sample GAPOKTAN (5 pertama): {sample_gapoktan}")
                        break
                if kec_col:
                    break
        
        # 3. Jika tidak ada GAPOKTAN, cari kolom lain yang mungkin berisi info wilayah
        if not kec_col:
            wilayah_patterns = ['DESA', 'KELURAHAN', 'DUSUN', 'KAMPUNG', 'NAMA DESA', 'DESA/KELURAHAN']
            for col in df.columns:
                col_upper = col.upper()
                for pattern in wilayah_patterns:
                    if pattern in col_upper:
                        kec_col = col
                        print(f"   ⚠️  Menggunakan kolom {col} sebagai Kecamatan")
                        break
                if kec_col:
                    break
        
        # 4. Jika masih tidak ditemukan, gunakan kolom POKTAN
        if not kec_col:
            poktan_patterns = ['POKTAN', 'KELOMPOK TANI', 'NAMA POKTAN']
            for col in df.columns:
                col_upper = col.upper()
                for pattern in poktan_patterns:
                    if pattern in col_upper:
                        kec_col = col
                        print(f"   ⚠️  Menggunakan kolom {col} sebagai Kecamatan")
                        break
                if kec_col:
                    break
        
        # 5. Jika semua gagal, gunakan kolom pertama yang bukan KTP/NIK
        if not kec_col:
            for col in df.columns:
                if col != ktp_col and col != nama_col:
                    kec_col = col
                    print(f"   ⚠️  Menggunakan kolom {col} sebagai Kecamatan (default)")
                    break
        
        print(f"   🔍 Kolom yang digunakan sebagai Kecamatan: {kec_col}")
        
        # ============================================
        # CARI KOLOM PUPUK
        # ============================================
        print(f"\n   🔍 Mencari kolom pupuk...")
        
        # Dictionary untuk menyimpan kolom pupuk per MT
        pupuk_columns = {
            'UREA': [],
            'NPK': [],
            'SP36': [],
            'ZA': [],
            'NPK_FORMULA': [],
            'ORGANIK': [],
            'ORGANIK_CAIR': []
        }
        
        # Pattern untuk setiap jenis pupuk
        pupuk_patterns = {
            'UREA': [r'UREA', r'UERA'],
            'NPK': [r'NPK(?!.*FORMULA)', r'NPK\s+[^F]'],  # NPK tapi bukan NPK FORMULA
            'SP36': [r'SP36', r'SP-36'],
            'ZA': [r'ZA'],
            'NPK_FORMULA': [r'NPK.*FORMULA', r'FORMULA.*NPK'],
            'ORGANIK': [r'ORGANIK(?!.*CAIR)', r'ORGANIK\s+[^C]'],  # ORGANIK tapi bukan ORGANIK CAIR
            'ORGANIK_CAIR': [r'ORGANIK.*CAIR', r'CAIR.*ORGANIK']
        }
        
        # Cari semua kolom yang mengandung kata kunci pupuk
        for col in df.columns:
            col_upper = str(col).upper()
            
            for pupuk_type, patterns in pupuk_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, col_upper, re.IGNORECASE):
                        pupuk_columns[pupuk_type].append(col)
                        break
        
        # Tampilkan kolom yang ditemukan
        found_any = False
        for pupuk_type, cols in pupuk_columns.items():
            if cols:
                found_any = True
                print(f"   ✅ {pupuk_type}: {len(cols)} kolom ditemukan")
                if len(cols) <= 3:
                    for col in cols[:3]:
                        print(f"      - {col}")
            else:
                print(f"   ⚠️  {pupuk_type}: Tidak ditemukan kolom")
        
        # ============================================
        # PROSES SETIAP BARIS
        # ============================================
        results = []
        skipped_rows = 0
        
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik_value = row.get(ktp_col, '')
                nik = clean_nik(nik_value)
                if not nik or len(nik) != 16:
                    skipped_rows += 1
                    if idx < 3:
                        print(f"   ⚠️  Baris {idx}: NIK '{nik_value}' tidak valid -> '{nik}'")
                    continue
                
                # Ambil nilai kecamatan dari kolom yang telah ditentukan
                kecamatan_value = row.get(kec_col, '') if kec_col else ''
                if pd.isna(kecamatan_value):
                    kecamatan_value = ''
                
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row.get(nama_col, '')).strip() if nama_col and pd.notna(row.get(nama_col)) else '',
                    'KECAMATAN': str(kecamatan_value).strip().upper(),
                    'KODE_KIOS': str(row.get(kode_kios_col, '')).strip().upper() if kode_kios_col and pd.notna(row.get(kode_kios_col)) else '',
                    'NAMA_KIOS': str(row.get(nama_kios_col, '')).strip() if nama_kios_col and pd.notna(row.get(nama_kios_col)) else '',
                    'TOTAL_UREA': 0,
                    'TOTAL_NPK': 0,
                    'TOTAL_SP36': 0,
                    'TOTAL_ZA': 0,
                    'TOTAL_NPK_FORMULA': 0,
                    'TOTAL_ORGANIK': 0,
                    'TOTAL_ORGANIK_CAIR': 0,
                    'FILE_SOURCE': file_name
                }
                
                # Hitung total per jenis pupuk dari semua kolom yang ditemukan
                for pupuk_type, cols in pupuk_columns.items():
                    if not cols:
                        continue
                    
                    total = 0
                    for col in cols:
                        value = row.get(col)
                        if pd.notna(value):
                            try:
                                # Coba konversi ke float
                                if isinstance(value, (int, float)):
                                    num_value = float(value)
                                elif isinstance(value, str):
                                    # Bersihkan string dari karakter non-numeric
                                    clean_str = re.sub(r'[^\d.-]', '', value)
                                    if clean_str:
                                        num_value = float(clean_str)
                                    else:
                                        num_value = 0
                                else:
                                    num_value = 0
                                
                                total += num_value
                            except (ValueError, TypeError):
                                # Jika tidak bisa dikonversi, coba parsing string
                                if isinstance(value, str):
                                    # Cari angka dalam string
                                    numbers = re.findall(r'\d+\.?\d*', value)
                                    if numbers:
                                        try:
                                            num_value = float(numbers[0])
                                            total += num_value
                                        except:
                                            pass
                    
                    # Simpan total per jenis pupuk
                    if pupuk_type == 'UREA':
                        result['TOTAL_UREA'] = total
                    elif pupuk_type == 'NPK':
                        result['TOTAL_NPK'] = total
                    elif pupuk_type == 'SP36':
                        result['TOTAL_SP36'] = total
                    elif pupuk_type == 'ZA':
                        result['TOTAL_ZA'] = total
                    elif pupuk_type == 'NPK_FORMULA':
                        result['TOTAL_NPK_FORMULA'] = total
                    elif pupuk_type == 'ORGANIK':
                        result['TOTAL_ORGANIK'] = total
                    elif pupuk_type == 'ORGANIK_CAIR':
                        result['TOTAL_ORGANIK_CAIR'] = total
                
                # Cek apakah ada data pupuk
                has_pupuk_data = any([
                    result['TOTAL_UREA'] > 0,
                    result['TOTAL_NPK'] > 0,
                    result['TOTAL_SP36'] > 0,
                    result['TOTAL_ZA'] > 0,
                    result['TOTAL_NPK_FORMULA'] > 0,
                    result['TOTAL_ORGANIK'] > 0,
                    result['TOTAL_ORGANIK_CAIR'] > 0
                ])
                
                if has_pupuk_data:
                    results.append(result)
                else:
                    skipped_rows += 1
                    if idx < 3:
                        print(f"   ⚠️  Baris {idx}: Tidak ada data pupuk")
                
            except Exception as e:
                if idx < 3:
                    print(f"   ⚠️  Error processing row {idx}: {e}")
                skipped_rows += 1
                continue
        
        print(f"   ✅ Berhasil diproses: {len(results)} baris data")
        if skipped_rows > 0:
            print(f"   ⚠️  Dilewati: {skipped_rows} baris (NIK tidak valid/tidak ada data pupuk)")
        
        # Tampilkan sample dengan detail
        if results:
            print(f"\n   🔍 Sample data (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     NAMA: {sample['NAMA_PETANI'][:30]}{'...' if len(sample['NAMA_PETANI']) > 30 else ''}")
            print(f"     KECAMATAN: {sample['KECAMATAN']}")
            print(f"     KODE_KIOS: {sample['KODE_KIOS']}")
            print(f"     UREA: {sample['TOTAL_UREA']:.2f} Kg")
            print(f"     NPK: {sample['TOTAL_NPK']:.2f} Kg")
            
            # Hitung total untuk verifikasi
            total_urea = sum(r['TOTAL_UREA'] for r in results)
            total_npk = sum(r['TOTAL_NPK'] for r in results)
            print(f"\n   📊 Total dalam file ini:")
            print(f"     Total UREA: {total_urea:.2f} Kg")
            print(f"     Total NPK: {total_npk:.2f} Kg")
        
        return results

    except Exception as e:
        print(f"   ❌ Error memproses ERDKK {file_name}: {str(e)}")
        traceback.print_exc()
        return []

def aggregate_erdkk_by_kecamatan(all_erdkk_rows):
    """Agregasi data ERDKK per Kecamatan"""
    if not all_erdkk_rows:
        print("⚠️  Tidak ada data ERDKK untuk diagregasi")
        return pd.DataFrame()

    print("\n📊 Mengagregasi data ERDKK per KECAMATAN...")
    df = pd.DataFrame(all_erdkk_rows)
    
    # Handle kasus KECAMATAN kosong
    if 'KECAMATAN' not in df.columns or df['KECAMATAN'].isna().all():
        print("⚠️  Kolom KECAMATAN tidak ada atau semua kosong")
        print("ℹ️  Akan menggunakan 'TIDAK DIKETAHUI' sebagai kecamatan")
        df['KECAMATAN'] = 'TIDAK DIKETAHUI'
    
    # Pastikan KECAMATAN tidak null
    df['KECAMATAN'] = df['KECAMATAN'].fillna('TIDAK DIKETAHUI')
    df = df[df['KECAMATAN'] != '']
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN yang valid")
        return pd.DataFrame()
    
    # Group by KECAMATAN
    agg_dict = {
        'TOTAL_UREA': 'sum',
        'TOTAL_NPK': 'sum',
        'TOTAL_SP36': 'sum',
        'TOTAL_ZA': 'sum',
        'TOTAL_NPK_FORMULA': 'sum',
        'TOTAL_ORGANIK': 'sum',
        'TOTAL_ORGANIK_CAIR': 'sum'
    }
    
    kec_df = df.groupby(['KECAMATAN']).agg(agg_dict).reset_index()
    
    # Round values
    pupuk_cols = ['TOTAL_UREA', 'TOTAL_NPK', 'TOTAL_SP36', 'TOTAL_ZA', 
                  'TOTAL_NPK_FORMULA', 'TOTAL_ORGANIK', 'TOTAL_ORGANIK_CAIR']
    
    for col in pupuk_cols:
        kec_df[col] = kec_df[col].round(2)
    
    # Urutkan kolom
    kec_df = kec_df[['KECAMATAN'] + pupuk_cols]
    
    # Sort by KECAMATAN
    kec_df = kec_df.sort_values('KECAMATAN')
    
    print(f"✅ Agregasi kecamatan selesai: {len(kec_df)} baris")
    
    if len(kec_df) > 0:
        print(f"\n📊 Sample agregasi kecamatan (3 pertama):")
        print(kec_df.head(3).to_string())
        
        # Hitung total semua kecamatan
        print(f"\n📊 Total semua kecamatan:")
        for col in pupuk_cols:
            total = kec_df[col].sum()
            print(f"   • {col}: {total:,.2f} Kg")
    
    return kec_df

def aggregate_erdkk_by_kios(all_erdkk_rows):
    """Agregasi data ERDKK per Kode Kios"""
    if not all_erdkk_rows:
        print("⚠️  Tidak ada data ERDKK untuk diagregasi")
        return pd.DataFrame()

    print("\n📊 Mengagregasi data ERDKK per KIOS...")
    df = pd.DataFrame(all_erdkk_rows)
    
    # Filter yang punya KECAMATAN dan KODE_KIOS
    mask = df['KECAMATAN'].notna() & (df['KECAMATAN'] != '') & df['KODE_KIOS'].notna() & (df['KODE_KIOS'] != '')
    df = df[mask]
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN dan KODE_KIOS yang valid")
        return pd.DataFrame()
    
    # Group by KECAMATAN dan KODE_KIOS
    agg_dict = {
        'NAMA_KIOS': 'first',
        'TOTAL_UREA': 'sum',
        'TOTAL_NPK': 'sum',
        'TOTAL_SP36': 'sum',
        'TOTAL_ZA': 'sum',
        'TOTAL_NPK_FORMULA': 'sum',
        'TOTAL_ORGANIK': 'sum',
        'TOTAL_ORGANIK_CAIR': 'sum'
    }
    
    kios_df = df.groupby(['KECAMATAN', 'KODE_KIOS']).agg(agg_dict).reset_index()
    
    # Round values
    pupuk_cols = ['TOTAL_UREA', 'TOTAL_NPK', 'TOTAL_SP36', 'TOTAL_ZA', 
                  'TOTAL_NPK_FORMULA', 'TOTAL_ORGANIK', 'TOTAL_ORGANIK_CAIR']
    
    for col in pupuk_cols:
        kios_df[col] = kios_df[col].round(2)
    
    # Urutkan kolom
    kios_df = kios_df[['KECAMATAN', 'KODE_KIOS', 'NAMA_KIOS'] + pupuk_cols]
    
    # Sort by KECAMATAN then KODE_KIOS
    kios_df = kios_df.sort_values(['KECAMATAN', 'KODE_KIOS'])
    
    print(f"✅ Agregasi kios selesai: {len(kios_df)} baris")
    
    if len(kios_df) > 0:
        print(f"\n📊 Sample agregasi kios (3 pertama):")
        print(kios_df.head(3).to_string())
    
    return kios_df

# ============================
# FUNGSI PROSES DATA REALISASI DENGAN TANGGAL INPUT
# ============================
def process_realisasi_file(file_path, file_name):
    """Proses satu file realisasi dengan ekstraksi tanggal input"""
    try:
        print(f"\n   📖 Memproses Realisasi: {file_name}")

        # Ekstrak tanggal input terbaru dari file
        latest_tanggal_input = extract_latest_tanggal_input_from_file(file_path)
        
        # Baca file Excel dengan dtype=str
        df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada (15 pertama): {list(df.columns)[:15]}")
        
        # ============================================
        # IDENTIFIKASI KOLOM YANG LEBIH AKURAT
        # ============================================
        
        # Cari kolom NIK/KTP - PRIORITAS TINGGI
        nik_col = ''
        
        # Pattern untuk NIK
        nik_patterns = ['NIK', 'KTP', 'NOMOR INDUK KEPENDUDUKAN']
        
        for col in df.columns:
            col_upper = col.upper()
            for pattern in nik_patterns:
                if pattern in col_upper:
                    nik_col = col
                    break
            if nik_col:
                break
        
        # Jika belum ketemu, cari kolom yang berisi angka 16 digit
        if not nik_col:
            for col in df.columns[:10]:  # Cek 10 kolom pertama
                if df[col].notna().any():
                    sample = df[col].head(3).astype(str).tolist()
                    # Cek jika sample mengandung angka 16 digit
                    for val in sample:
                        if isinstance(val, str):
                            digits = re.sub(r'\D', '', val)
                            if len(digits) == 16:
                                nik_col = col
                                break
                if nik_col:
                    break
        
        # Cari kolom lainnya
        nama_col = ''
        kec_col = ''
        kode_kios_col = ''
        nama_kios_col = ''
        status_col = ''
        
        # Pattern untuk kolom lain
        column_patterns = {
            'nama': ['NAMA PETANI', 'NAMA'],
            'kecamatan': ['KECAMATAN', 'KEC'],
            'kode_kios': ['KODE KIOS', 'KODE'],
            'nama_kios': ['NAMA KIOS', 'NAMA PENGECER'],
            'status': ['STATUS', 'STATUS PENGAJUAN']
        }
        
        for col in df.columns:
            col_upper = col.upper()
            
            # Skip jika ini kolom NIK
            if col == nik_col:
                continue
            
            # Cari kolom nama
            if not nama_col:
                for pattern in column_patterns['nama']:
                    if pattern in col_upper:
                        nama_col = col
                        break
            
            # Cari kolom kecamatan
            if not kec_col:
                for pattern in column_patterns['kecamatan']:
                    if pattern in col_upper:
                        kec_col = col
                        break
            
            # Cari kolom kode kios
            if not kode_kios_col:
                for pattern in column_patterns['kode_kios']:
                    if pattern in col_upper:
                        kode_kios_col = col
                        break
            
            # Cari kolom nama kios
            if not nama_kios_col:
                for pattern in column_patterns['nama_kios']:
                    if pattern in col_upper:
                        nama_kios_col = col
                        break
            
            # Cari kolom status
            if not status_col:
                for pattern in column_patterns['status']:
                    if pattern in col_upper:
                        status_col = col
                        break
        
        print(f"   🔍 Kolom yang teridentifikasi (REVISI):")
        print(f"     NIK: {nik_col if nik_col else 'TIDAK DITEMUKAN'}")
        print(f"     NAMA: {nama_col if nama_col else 'TIDAK DITEMUKAN'}")
        print(f"     KECAMATAN: {kec_col if kec_col else 'TIDAK DITEMUKAN'}")
        print(f"     KODE_KIOS: {kode_kios_col if kode_kios_col else 'TIDAK DITEMUKAN'}")
        print(f"     NAMA_KIOS: {nama_kios_col if nama_kios_col else 'TIDAK DITEMUKAN'}")
        print(f"     STATUS: {status_col if status_col else 'TIDAK DITEMUKAN'}")
        
        # Tampilkan sample data untuk verifikasi
        if nik_col and len(df) > 0:
            sample_niks = df[nik_col].head(3).astype(str).tolist()
            print(f"   🔍 Sample NIK (3 pertama):")
            for i, nik in enumerate(sample_niks):
                cleaned = clean_nik(nik)
                print(f"     {i+1}. '{nik}' -> clean: '{cleaned}' (panjang: {len(cleaned) if cleaned else 0})")
        
        # ============================================
        # IDENTIFIKASI KOLOM PUPUK
        # ============================================
        
        # Cari kolom pupuk dengan pattern yang lebih spesifik
        pupuk_patterns = {
            'UREA': r'UREA',
            'NPK': r'NPK(?!.*FORMULA)',  # NPK tapi bukan NPK FORMULA
            'SP36': r'SP36|SP-36',
            'ZA': r'ZA',
            'NPK_FORMULA': r'NPK.*FORMULA|FORMULA.*NPK',
            'ORGANIK': r'ORGANIK(?!.*CAIR)',  # ORGANIK tapi bukan ORGANIK CAIR
            'ORGANIK_CAIR': r'ORGANIK.*CAIR|CAIR.*ORGANIK'
        }
        
        pupuk_cols_found = {}
        for pupuk_type, pattern in pupuk_patterns.items():
            matches = [col for col in df.columns if re.search(pattern, col.upper(), re.IGNORECASE)]
            if matches:
                # Pilih kolom pertama yang cocok
                pupuk_cols_found[pupuk_type] = matches[0]
        
        print(f"   🔍 Kolom pupuk yang ditemukan:")
        for pupuk_type, col_name in pupuk_cols_found.items():
            print(f"     {pupuk_type}: {col_name}")
        
        # ============================================
        # PROSES DATA
        # ============================================
        
        # Jika NIK tidak ditemukan, coba kolom pertama
        if not nik_col and len(df.columns) > 0:
            nik_col = df.columns[0]
            print(f"   ⚠️  Menggunakan kolom pertama sebagai NIK: {nik_col}")
        
        if not nik_col:
            print(f"   ❌ Tidak dapat menemukan kolom NIK, melewati file ini")
            return []
        
        results = []
        skipped_rows = 0
        valid_rows = 0
        
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik_value = str(row[nik_col]) if nik_col in row else ''
                nik = clean_nik(nik_value)
                
                # Validasi NIK - harus 16 digit
                if not nik or len(nik) != 16:
                    skipped_rows += 1
                    if idx < 3:  # Log hanya untuk 3 baris pertama
                        print(f"   ⚠️  Baris {idx}: NIK '{nik_value}' tidak valid -> '{nik}'")
                    continue
                
                # Build result dictionary dengan tanggal input
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row[nama_col]).strip() if nama_col and nama_col in row and pd.notna(row[nama_col]) else '',
                    'KECAMATAN': str(row[kec_col]).strip().upper() if kec_col and kec_col in row and pd.notna(row[kec_col]) else '',
                    'KODE_KIOS': str(row[kode_kios_col]).strip().upper() if kode_kios_col and kode_kios_col in row and pd.notna(row[kode_kios_col]) else '',
                    'NAMA_KIOS': str(row[nama_kios_col]).strip() if nama_kios_col and nama_kios_col in row and pd.notna(row[nama_kios_col]) else '',
                    'STATUS': str(row[status_col]).strip() if status_col and status_col in row and pd.notna(row[status_col]) else '',
                    'REALISASI_UREA': 0,
                    'REALISASI_NPK': 0,
                    'REALISASI_SP36': 0,
                    'REALISASI_ZA': 0,
                    'REALISASI_NPK_FORMULA': 0,
                    'REALISASI_ORGANIK': 0,
                    'REALISASI_ORGANIK_CAIR': 0,
                    'FILE_SOURCE': file_name,
                    'TANGGAL_INPUT': latest_tanggal_input  # Tambahkan tanggal input
                }
                
                # Hitung realisasi pupuk
                for pupuk_type, col_name in pupuk_cols_found.items():
                    if col_name in row and pd.notna(row[col_name]):
                        try:
                            value = str(row[col_name])
                            # Bersihkan dari karakter non-numeric
                            clean_value = re.sub(r'[^\d.-]', '', value)
                            if clean_value:
                                num_value = float(clean_value)
                            else:
                                num_value = 0
                        except:
                            num_value = 0
                        
                        if pupuk_type == 'UREA':
                            result['REALISASI_UREA'] = num_value
                        elif pupuk_type == 'NPK':
                            result['REALISASI_NPK'] = num_value
                        elif pupuk_type == 'SP36':
                            result['REALISASI_SP36'] = num_value
                        elif pupuk_type == 'ZA':
                            result['REALISASI_ZA'] = num_value
                        elif pupuk_type == 'NPK_FORMULA':
                            result['REALISASI_NPK_FORMULA'] = num_value
                        elif pupuk_type == 'ORGANIK':
                            result['REALISASI_ORGANIK'] = num_value
                        elif pupuk_type == 'ORGANIK_CAIR':
                            result['REALISASI_ORGANIK_CAIR'] = num_value
                
                results.append(result)
                valid_rows += 1
                
            except Exception as e:
                skipped_rows += 1
                if idx < 3:
                    print(f"   ⚠️  Error processing row {idx}: {e}")
                continue
        
        print(f"   ✅ Berhasil: {valid_rows} baris data valid")
        if skipped_rows > 0:
            print(f"   ⚠️  Dilewati: {skipped_rows} baris (NIK tidak valid/error)")
        
        # Tampilkan informasi tanggal input
        if latest_tanggal_input:
            print(f"   📅 Tanggal input terbaru dari file ini: {latest_tanggal_input.strftime('%d %b %Y %H:%M:%S')}")
        else:
            print(f"   ⚠️  Tidak dapat menemukan tanggal input dalam file ini")
        
        # Tampilkan sample
        if results:
            print(f"\n   🔍 Sample data (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     NAMA: {sample['NAMA_PETANI'][:30]}{'...' if len(sample['NAMA_PETANI']) > 30 else ''}")
            print(f"     STATUS: {sample['STATUS']}")
            print(f"     KECAMATAN: {sample['KECAMATAN']}")
            print(f"     UREA: {sample['REALISASI_UREA']}")
            print(f"     NPK: {sample['REALISASI_NPK']}")
            print(f"     Is ACC PUSAT? {is_status_disetujui_pusat(sample['STATUS'])}")
            if sample.get('TANGGAL_INPUT'):
                print(f"     TANGGAL INPUT: {sample['TANGGAL_INPUT'].strftime('%d %b %Y %H:%M:%S')}")
        
        return results

    except Exception as e:
        print(f"   ❌ Error memproses realisasi {file_name}: {str(e)}")
        traceback.print_exc()
        return []

def aggregate_realisasi_by_kecamatan(all_realisasi_rows, filter_acc_pusat=False):
    """Agregasi data realisasi per Kecamatan"""
    if not all_realisasi_rows:
        print(f"⚠️  Tidak ada data realisasi untuk diagregasi (filter: {'ACC PUSAT' if filter_acc_pusat else 'ALL'})")
        return pd.DataFrame(columns=['KECAMATAN', 'REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 
                                     'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])

    print(f"\n📊 Mengagregasi data REALISASI per KECAMATAN ({'ACC PUSAT' if filter_acc_pusat else 'ALL'})...")
    df = pd.DataFrame(all_realisasi_rows)
    
    # Filter berdasarkan status ACC PUSAT jika diperlukan
    if filter_acc_pusat:
        if 'STATUS' in df.columns:
            initial_count = len(df)
            mask = df['STATUS'].apply(is_status_disetujui_pusat)
            df = df[mask]
            print(f"   Filter ACC PUSAT: {len(df)}/{initial_count} baris tersisa")
        else:
            print(f"   ⚠️  Kolom STATUS tidak ditemukan, tidak bisa filter ACC PUSAT")
    
    if df.empty:
        print(f"   ⚠️  Tidak ada data setelah filter")
        return pd.DataFrame(columns=['KECAMATAN', 'REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 
                                     'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])
    
    # Handle kasus KECAMATAN kosong
    if 'KECAMATAN' not in df.columns or df['KECAMATAN'].isna().all():
        print(f"   ⚠️  Kolom KECAMATAN tidak ada atau semua kosong")
        df['KECAMATAN'] = 'TIDAK DIKETAHUI'
    
    # Pastikan KECAMATAN tidak null
    df['KECAMATAN'] = df['KECAMATAN'].fillna('TIDAK DIKETAHUI')
    df = df[df['KECAMATAN'] != '']
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN yang valid")
        return pd.DataFrame(columns=['KECAMATAN', 'REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 
                                     'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])

    # Group by KECAMATAN
    agg_dict = {
        'REALISASI_UREA': 'sum',
        'REALISASI_NPK': 'sum',
        'REALISASI_SP36': 'sum',
        'REALISASI_ZA': 'sum',
        'REALISASI_NPK_FORMULA': 'sum',
        'REALISASI_ORGANIK': 'sum',
        'REALISASI_ORGANIK_CAIR': 'sum'
    }
    
    kec_df = df.groupby(['KECAMATAN']).agg(agg_dict).reset_index()
    
    # Round values
    pupuk_cols = ['REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 'REALISASI_ZA', 
                  'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']
    
    for col in pupuk_cols:
        kec_df[col] = kec_df[col].round(2)
    
    # Urutkan kolom
    kec_df = kec_df[['KECAMATAN'] + pupuk_cols]
    
    # Sort by KECAMATAN
    kec_df = kec_df.sort_values('KECAMATAN')
    
    print(f"✅ Agregasi realisasi kecamatan selesai: {len(kec_df)} baris")
    
    if len(kec_df) > 0:
        print(f"\n📊 Sample agregasi realisasi kecamatan:")
        print(kec_df.head(3).to_string())
    
    return kec_df

def aggregate_realisasi_by_kios(all_realisasi_rows, filter_acc_pusat=False):
    """Agregasi data realisasi per Kode Kios"""
    if not all_realisasi_rows:
        print(f"⚠️  Tidak ada data realisasi untuk diagregasi (filter: {'ACC PUSAT' if filter_acc_pusat else 'ALL'})")
        return pd.DataFrame(columns=['KECAMATAN', 'KODE_KIOS', 'NAMA_KIOS', 'REALISASI_UREA', 'REALISASI_NPK', 
                                     'REALISASI_SP36', 'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 
                                     'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])

    print(f"\n📊 Mengagregasi data REALISASI per KIOS ({'ACC PUSAT' if filter_acc_pusat else 'ALL'})...")
    df = pd.DataFrame(all_realisasi_rows)
    
    # Filter berdasarkan status ACC PUSAT jika diperlukan
    if filter_acc_pusat:
        if 'STATUS' in df.columns:
            initial_count = len(df)
            mask = df['STATUS'].apply(is_status_disetujui_pusat)
            df = df[mask]
            print(f"   Filter ACC PUSAT: {len(df)}/{initial_count} baris tersisa")
        else:
            print(f"   ⚠️  Kolom STATUS tidak ditemukan, tidak bisa filter ACC PUSAT")
    
    if df.empty:
        print(f"   ⚠️  Tidak ada data setelah filter")
        return pd.DataFrame(columns=['KECAMATAN', 'KODE_KIOS', 'NAMA_KIOS', 'REALISASI_UREA', 'REALISASI_NPK', 
                                     'REALISASI_SP36', 'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 
                                     'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])
    
    # Handle kasus KECAMATAN kosong
    if 'KECAMATAN' not in df.columns or df['KECAMATAN'].isna().all():
        print(f"   ⚠️  Kolom KECAMATAN tidak ada atau semua kosong")
        df['KECAMATAN'] = 'TIDAK DIKETAHUI'
    
    # Filter yang punya KECAMATAN dan KODE_KIOS
    df['KECAMATAN'] = df['KECAMATAN'].fillna('TIDAK DIKETAHUI')
    mask = (df['KECAMATAN'] != '') & df['KODE_KIOS'].notna() & (df['KODE_KIOS'] != '')
    df = df[mask]
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN dan KODE_KIOS yang valid")
        return pd.DataFrame(columns=['KECAMATAN', 'KODE_KIOS', 'NAMA_KIOS', 'REALISASI_UREA', 'REALISASI_NPK', 
                                     'REALISASI_SP36', 'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 
                                     'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR'])

    # Group by KECAMATAN dan KODE_KIOS
    agg_dict = {
        'NAMA_KIOS': 'first',
        'REALISASI_UREA': 'sum',
        'REALISASI_NPK': 'sum',
        'REALISASI_SP36': 'sum',
        'REALISASI_ZA': 'sum',
        'REALISASI_NPK_FORMULA': 'sum',
        'REALISASI_ORGANIK': 'sum',
        'REALISASI_ORGANIK_CAIR': 'sum'
    }
    
    kios_df = df.groupby(['KECAMATAN', 'KODE_KIOS']).agg(agg_dict).reset_index()
    
    # Round values
    pupuk_cols = ['REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 'REALISASI_ZA', 
                  'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']
    
    for col in pupuk_cols:
        kios_df[col] = kios_df[col].round(2)
    
    # Urutkan kolom
    kios_df = kios_df[['KECAMATAN', 'KODE_KIOS', 'NAMA_KIOS'] + pupuk_cols]
    
    # Sort by KECAMATAN then KODE_KIOS
    kios_df = kios_df.sort_values(['KECAMATAN', 'KODE_KIOS'])
    
    print(f"✅ Agregasi realisasi kios selesai: {len(kios_df)} baris")
    
    if len(kios_df) > 0:
        print(f"\n📊 Sample agregasi realisasi kios:")
        print(kios_df.head(3).to_string())
    
    return kios_df

# ============================
# FUNGSI BUAT PERBANDINGAN
# ============================
def create_comparison_kecamatan(erdkk_kec_df, realisasi_kec_df_all, realisasi_kec_df_acc, latest_tanggal_input=None):
    """Buat tabel perbandingan untuk level kecamatan dengan struktur yang benar"""
    print("\n🔍 Membuat tabel perbandingan KECAMATAN...")
    
    if erdkk_kec_df.empty:
        print("⚠️  Data ERDKK kecamatan kosong")
        return pd.DataFrame(), pd.DataFrame()
    
    # Daftar jenis pupuk
    pupuk_types = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK_FORMULA', 'ORGANIK', 'ORGANIK_CAIR']
    
    # Inisialisasi DataFrames hasil
    comparison_all = pd.DataFrame()
    comparison_acc = pd.DataFrame()
    
    # Tambahkan kolom KECAMATAN
    comparison_all['KECAMATAN'] = erdkk_kec_df['KECAMATAN']
    comparison_acc['KECAMATAN'] = erdkk_kec_df['KECAMATAN']
    
    # Buat mapping untuk kolom ERDKK
    erdkk_cols = {
        'UREA': 'TOTAL_UREA',
        'NPK': 'TOTAL_NPK',
        'SP36': 'TOTAL_SP36',
        'ZA': 'TOTAL_ZA',
        'NPK_FORMULA': 'TOTAL_NPK_FORMULA',
        'ORGANIK': 'TOTAL_ORGANIK',
        'ORGANIK_CAIR': 'TOTAL_ORGANIK_CAIR'
    }
    
    # Buat mapping untuk kolom REALISASI
    real_cols = {
        'UREA': 'REALISASI_UREA',
        'NPK': 'REALISASI_NPK',
        'SP36': 'REALISASI_SP36',
        'ZA': 'REALISASI_ZA',
        'NPK_FORMULA': 'REALISASI_NPK_FORMULA',
        'ORGANIK': 'REALISASI_ORGANIK',
        'ORGANIK_CAIR': 'REALISASI_ORGANIK_CAIR'
    }
    
    for pupuk in pupuk_types:
        erdkk_col = erdkk_cols[pupuk]
        real_col = real_cols[pupuk]
        
        # Untuk ALL
        if erdkk_col in erdkk_kec_df.columns:
            # Kolom 1: ERDKK
            comparison_all[f'{pupuk} ERDKK'] = erdkk_kec_df[erdkk_col].fillna(0)
            
            # Kolom 2: REALISASI (semua status)
            if not realisasi_kec_df_all.empty and real_col in realisasi_kec_df_all.columns:
                # Gabungkan data
                merged = pd.merge(
                    erdkk_kec_df[['KECAMATAN', erdkk_col]],
                    realisasi_kec_df_all[['KECAMATAN', real_col]],
                    on='KECAMATAN',
                    how='left'
                )
                comparison_all[f'{pupuk} REALISASI'] = merged[real_col].fillna(0)
            else:
                comparison_all[f'{pupuk} REALISASI'] = 0
            
            # Kolom 3: SELISIH (ERDKK - REALISASI)
            comparison_all[f'{pupuk} SELISIH'] = (
                comparison_all[f'{pupuk} ERDKK'] - comparison_all[f'{pupuk} REALISASI']
            )
            
            # Kolom 4: PERSENTASE (REALISASI/ERDKK) - DIUBAH MENJADI DESIMAL
            mask = comparison_all[f'{pupuk} ERDKK'] > 0
            comparison_all[f'{pupuk} %'] = 0
            comparison_all.loc[mask, f'{pupuk} %'] = (
                comparison_all.loc[mask, f'{pupuk} REALISASI'] / 
                comparison_all.loc[mask, f'{pupuk} ERDKK']
            )  # Hasilnya desimal (0.6106 untuk 61.06%)
        
        # Untuk ACC PUSAT - SAMA SEKALIPUN DATA KOSONG
        if erdkk_col in erdkk_kec_df.columns:
            # Kolom 1: ERDKK
            comparison_acc[f'{pupuk} ERDKK'] = erdkk_kec_df[erdkk_col].fillna(0)
            
            # Kolom 2: REALISASI (ACC PUSAT saja)
            if not realisasi_kec_df_acc.empty and real_col in realisasi_kec_df_acc.columns:
                # Gabungkan data
                merged = pd.merge(
                    erdkk_kec_df[['KECAMATAN', erdkk_col]],
                    realisasi_kec_df_acc[['KECAMATAN', real_col]],
                    on='KECAMATAN',
                    how='left'
                )
                comparison_acc[f'{pupuk} REALISASI'] = merged[real_col].fillna(0)
            else:
                # Jika data ACC PUSAT kosong, set REALISASI = 0
                comparison_acc[f'{pupuk} REALISASI'] = 0
            
            # Kolom 3: SELISIH (ERDKK - REALISASI)
            comparison_acc[f'{pupuk} SELISIH'] = (
                comparison_acc[f'{pupuk} ERDKK'] - comparison_acc[f'{pupuk} REALISASI']
            )
            
            # Kolom 4: PERSENTASE (REALISASI/ERDKK) - DIUBAH MENJADI DESIMAL
            mask = comparison_acc[f'{pupuk} ERDKK'] > 0
            comparison_acc[f'{pupuk} %'] = 0
            comparison_acc.loc[mask, f'{pupuk} %'] = (
                comparison_acc.loc[mask, f'{pupuk} REALISASI'] / 
                comparison_acc.loc[mask, f'{pupuk} ERDKK']
            )  # Hasilnya desimal
    
    # Format angka dengan 2 desimal
    number_cols = [col for col in comparison_all.columns if any(x in col for x in ['ERDKK', 'REALISASI', 'SELISIH'])]
    for col in number_cols:
        comparison_all[col] = comparison_all[col].round(2)
        if col in comparison_acc.columns:
            comparison_acc[col] = comparison_acc[col].round(2)
    
    # Format persentase dengan 4 desimal (untuk konversi ke persen nanti)
    percent_cols = [col for col in comparison_all.columns if '%' in col]
    for col in percent_cols:
        comparison_all[col] = comparison_all[col].round(4)
        if col in comparison_acc.columns:
            comparison_acc[col] = comparison_acc[col].round(4)
    
    # Tambahkan baris TOTAL di akhir
    if not comparison_all.empty:
        # Buat dictionary untuk total
        total_row = {'KECAMATAN': 'TOTAL'}
        
        # Hitung total untuk setiap kolom numerik
        for col in comparison_all.columns:
            if col != 'KECAMATAN':
                if '%' in col:
                    # Untuk persentase, hitung rata-rata tertimbang
                    erdkk_col = col.replace(' %', ' ERDKK')
                    real_col = col.replace(' %', ' REALISASI')
                    
                    if erdkk_col in comparison_all.columns and real_col in comparison_all.columns:
                        total_erdkk = comparison_all[erdkk_col].sum()
                        total_real = comparison_all[real_col].sum()
                        total_percent = total_real / total_erdkk if total_erdkk > 0 else 0
                        total_row[col] = total_percent
                else:
                    total_row[col] = comparison_all[col].sum()
        
        # Konversi ke DataFrame dan tambahkan
        total_df = pd.DataFrame([total_row])
        comparison_all = pd.concat([comparison_all, total_df], ignore_index=True)
    
    if not comparison_acc.empty:
        # Buat dictionary untuk total
        total_row = {'KECAMATAN': 'TOTAL'}
        
        # Hitung total untuk setiap kolom numerik
        for col in comparison_acc.columns:
            if col != 'KECAMATAN':
                if '%' in col:
                    # Untuk persentase, hitung rata-rata tertimbang
                    erdkk_col = col.replace(' %', ' ERDKK')
                    real_col = col.replace(' %', ' REALISASI')
                    
                    if erdkk_col in comparison_acc.columns and real_col in comparison_acc.columns:
                        total_erdkk = comparison_acc[erdkk_col].sum()
                        total_real = comparison_acc[real_col].sum()
                        total_percent = total_real / total_erdkk if total_erdkk > 0 else 0
                        total_row[col] = total_percent
                else:
                    total_row[col] = comparison_acc[col].sum()
        
        # Konversi ke DataFrame dan tambahkan
        total_df = pd.DataFrame([total_row])
        comparison_acc = pd.concat([comparison_acc, total_df], ignore_index=True)
    
    print(f"✅ Tabel perbandingan kecamatan dibuat:")
    print(f"   • ALL: {len(comparison_all)} baris (termasuk TOTAL)")
    print(f"   • ACC PUSAT: {len(comparison_acc)} baris (termasuk TOTAL)")
    
    # Tambahkan informasi tanggal input terbaru
    if latest_tanggal_input:
        print(f"   📅 Tanggal input terbaru dari data realisasi: {latest_tanggal_input.strftime('%d %b %Y %H:%M:%S')}")
    
    return comparison_all, comparison_acc

def create_comparison_kios(erdkk_kios_df, realisasi_kios_df_all, realisasi_kios_df_acc):
    """Buat tabel perbandingan untuk level kios"""
    print("\n🔍 Membuat tabel perbandingan KIOS...")
    
    if erdkk_kios_df.empty:
        print("⚠️  Data ERDKK kios kosong")
        return pd.DataFrame(), pd.DataFrame()
    
    # Daftar jenis pupuk
    pupuk_types = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK_FORMULA', 'ORGANIK', 'ORGANIK_CAIR']
    
    # Inisialisasi DataFrames hasil
    comparison_all = pd.DataFrame()
    comparison_acc = pd.DataFrame()
    
    # Tambahkan kolom dasar
    comparison_all['KECAMATAN'] = erdkk_kios_df['KECAMATAN']
    comparison_all['KODE_KIOS'] = erdkk_kios_df['KODE_KIOS']
    comparison_all['NAMA_KIOS'] = erdkk_kios_df['NAMA_KIOS']
    
    comparison_acc['KECAMATAN'] = erdkk_kios_df['KECAMATAN']
    comparison_acc['KODE_KIOS'] = erdkk_kios_df['KODE_KIOS']
    comparison_acc['NAMA_KIOS'] = erdkk_kios_df['NAMA_KIOS']
    
    for pupuk in pupuk_types:
        erdkk_col = f'TOTAL_{pupuk}'
        real_col = f'REALISASI_{pupuk}'
        
        # Untuk ALL
        if erdkk_col in erdkk_kios_df.columns:
            # Kolom 1: ERDKK
            comparison_all[f'{pupuk} ERDKK'] = erdkk_kios_df[erdkk_col].fillna(0)
            
            # Kolom 2: REALISASI (semua status)
            if not realisasi_kios_df_all.empty and real_col in realisasi_kios_df_all.columns:
                # Gabungkan data
                merged = pd.merge(
                    erdkk_kios_df[['KECAMATAN', 'KODE_KIOS', erdkk_col]],
                    realisasi_kios_df_all[['KECAMATAN', 'KODE_KIOS', real_col]],
                    on=['KECAMATAN', 'KODE_KIOS'],
                    how='left'
                )
                comparison_all[f'{pupuk} REALISASI'] = merged[real_col].fillna(0)
            else:
                comparison_all[f'{pupuk} REALISASI'] = 0
            
            # Kolom 3: SELISIH (ERDKK - REALISASI)
            comparison_all[f'{pupuk} SELISIH'] = (
                comparison_all[f'{pupuk} ERDKK'] - comparison_all[f'{pupuk} REALISASI']
            )
            
            # Kolom 4: PERSENTASE (REALISASI/ERDKK) - DESIMAL
            mask = comparison_all[f'{pupuk} ERDKK'] > 0
            comparison_all[f'{pupuk} %'] = 0
            comparison_all.loc[mask, f'{pupuk} %'] = (
                comparison_all.loc[mask, f'{pupuk} REALISASI'] / 
                comparison_all.loc[mask, f'{pupuk} ERDKK']
            )
        
        # Untuk ACC PUSAT - SAMA SEKALIPUN DATA KOSONG
        if erdkk_col in erdkk_kios_df.columns:
            # Kolom 1: ERDKK
            comparison_acc[f'{pupuk} ERDKK'] = erdkk_kios_df[erdkk_col].fillna(0)
            
            # Kolom 2: REALISASI (ACC PUSAT saja)
            if not realisasi_kios_df_acc.empty and real_col in realisasi_kios_df_acc.columns:
                # Gabungkan data
                merged = pd.merge(
                    erdkk_kios_df[['KECAMATAN', 'KODE_KIOS', erdkk_col]],
                    realisasi_kios_df_acc[['KECAMATAN', 'KODE_KIOS', real_col]],
                    on=['KECAMATAN', 'KODE_KIOS'],
                    how='left'
                )
                comparison_acc[f'{pupuk} REALISASI'] = merged[real_col].fillna(0)
            else:
                # Jika data ACC PUSAT kosong, set REALISASI = 0
                comparison_acc[f'{pupuk} REALISASI'] = 0
            
            # Kolom 3: SELISIH (ERDKK - REALISASI)
            comparison_acc[f'{pupuk} SELISIH'] = (
                comparison_acc[f'{pupuk} ERDKK'] - comparison_acc[f'{pupuk} REALISASI']
            )
            
            # Kolom 4: PERSENTASE (REALISASI/ERDKK) - DESIMAL
            mask = comparison_acc[f'{pupuk} ERDKK'] > 0
            comparison_acc[f'{pupuk} %'] = 0
            comparison_acc.loc[mask, f'{pupuk} %'] = (
                comparison_acc.loc[mask, f'{pupuk} REALISASI'] / 
                comparison_acc.loc[mask, f'{pupuk} ERDKK']
            )
    
    # Format angka dengan 2 desimal
    number_cols = [col for col in comparison_all.columns if any(x in col for x in ['ERDKK', 'REALISASI', 'SELISIH'])]
    for col in number_cols:
        comparison_all[col] = comparison_all[col].round(2)
        if col in comparison_acc.columns:
            comparison_acc[col] = comparison_acc[col].round(2)
    
    # Format persentase dengan 4 desimal
    percent_cols = [col for col in comparison_all.columns if '%' in col]
    for col in percent_cols:
        comparison_all[col] = comparison_all[col].round(4)
        if col in comparison_acc.columns:
            comparison_acc[col] = comparison_acc[col].round(4)
    
    print(f"✅ Tabel perbandingan kios dibuat:")
    print(f"   • ALL: {len(comparison_all)} baris")
    print(f"   • ACC PUSAT: {len(comparison_acc)} baris")
    
    if len(comparison_all) > 0:
        print(f"\n📊 Sample data (3 baris pertama):")
        print(comparison_all.head(3).to_string())
    
    return comparison_all, comparison_acc

# ============================
# FUNGSI UPDATE GOOGLE SHEETS DENGAN TANGGAL INPUT
# ============================
def format_worksheet_with_date(worksheet, df, latest_tanggal_input=None):
    """Format worksheet dengan warna header, border, dan informasi tanggal"""
    try:
        # Format header (baris 1)
        header_format = {
            "backgroundColor": {
                "red": 0.2,
                "green": 0.6,
                "blue": 0.8
            },
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "bold": True,
                "fontSize": 11
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP"
        }
        
        # Format untuk baris TOTAL (baris terakhir)
        total_format = {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.9,
                "blue": 0.9
            },
            "textFormat": {
                "bold": True
            }
        }
        
        # Format untuk kolom persentase
        percent_format = {
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "0.00%"
            }
        }
        
        # Format untuk kolom angka
        number_format = {
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "#,##0.00"
            }
        }
        
        # Format untuk informasi tanggal (baris 1-3, kolom E)
        date_info_format = {
            "backgroundColor": {
                "red": 0.95,
                "green": 0.95,
                "blue": 0.85
            },
            "textFormat": {
                "bold": True,
                "fontSize": 10
            },
            "horizontalAlignment": "LEFT",
            "verticalAlignment": "MIDDLE"
        }
        
        # Format header
        worksheet.format("1:1", header_format)
        
        # Tambahkan informasi tanggal input di kolom E (kolom 5)
        if latest_tanggal_input:
            try:
                # E1: Label
                worksheet.update('E1', 'Update per tanggal input')
                # E2: Tanggal
                worksheet.update('E2', latest_tanggal_input.strftime('%d %b %Y'))
                # E3: Jam
                worksheet.update('E3', latest_tanggal_input.strftime('%H:%M:%S'))
                
                # Format kolom E
                worksheet.format('E1:E3', date_info_format)
                print(f"      ✅ Informasi tanggal ditambahkan: {latest_tanggal_input.strftime('%d %b %Y %H:%M:%S')}")
            except Exception as e:
                print(f"      ⚠️  Gagal menambahkan informasi tanggal: {e}")
        
        # Format baris TOTAL (jika ada)
        total_row = len(df) + 1  # +1 karena header di baris 1
        if 'KECAMATAN' in df.columns and 'TOTAL' in df['KECAMATAN'].values:
            worksheet.format(f"{total_row}:{total_row}", total_format)
        
        # Format kolom persentase
        for col_idx, col_name in enumerate(df.columns, start=1):
            if '%' in col_name:
                col_letter = gspread.utils.rowcol_to_a1(1, col_idx)[0]
                worksheet.format(f"{col_letter}2:{col_letter}{total_row}", percent_format)
            elif any(x in col_name for x in ['ERDKK', 'REALISASI', 'SELISIH']):
                col_letter = gspread.utils.rowcol_to_a1(1, col_idx)[0]
                worksheet.format(f"{col_letter}2:{col_letter}{total_row}", number_format)
        
        # Set lebar kolom otomatis
        try:
            worksheet.columns_auto_resize(start_column_index=0, end_column_index=len(df.columns))
        except:
            pass
        
        # Freeze header row
        try:
            worksheet.freeze(rows=1)
        except:
            pass
        
        print(f"      ✅ Formatting diterapkan")
        
    except Exception as e:
        print(f"      ⚠️  Gagal formatting: {e}")

def batch_update_worksheets_with_date(spreadsheet, updates, latest_tanggal_input=None):
    """Batch update untuk multiple worksheets dengan formatting dan tanggal"""
    print(f"🔄 Memproses batch update untuk {len(updates)} worksheet...")
    
    success_count = 0
    for i, (sheet_name, data) in enumerate(updates):
        try:
            print(f"   📝 Processing {i+1}/{len(updates)}: {sheet_name} ({len(data)} baris)")
            
            try:
                # Coba akses sheet yang sudah ada
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"      📝 Menggunakan sheet existing")
                
                # Clear existing data
                safe_google_api_operation(worksheet.clear)
                time.sleep(WRITE_DELAY)
                
            except gspread.exceptions.WorksheetNotFound:
                # Buat sheet baru
                worksheet = safe_google_api_operation(
                    spreadsheet.add_worksheet, 
                    title=sheet_name, 
                    rows=str(max(1000, len(data) + 100)), 
                    cols=str(min(50, len(data.columns) + 5))
                )
                print(f"      ✅ Membuat sheet baru: {sheet_name}")
                time.sleep(WRITE_DELAY)
            
            # Update data
            safe_google_api_operation(
                worksheet.update,
                [data.columns.values.tolist()] + data.values.tolist(),
                value_input_option='USER_ENTERED'
            )
            
            # Format worksheet dengan tanggal (hanya untuk sheet pertama/kecamatan_all)
            if sheet_name == "kecamatan_all":
                format_worksheet_with_date(worksheet, data, latest_tanggal_input)
            else:
                format_worksheet_with_date(worksheet, data, None)
            
            print(f"      ✅ Berhasil update data ({len(data)} baris, {len(data.columns)} kolom)")
            success_count += 1
            
            if i < len(updates) - 1:
                time.sleep(WRITE_DELAY)
                
        except Exception as e:
            print(f"      ❌ Gagal update {sheet_name}: {str(e)}")
            continue
    
    print(f"✅ Batch update selesai: {success_count}/{len(updates)} berhasil")
    return success_count

# ============================
# FUNGSI UTAMA DENGAN TANGGAL INPUT
# ============================
def process_erdkk_vs_realisasi_with_date():
    """Fungsi utama untuk analisis perbandingan ERDKK vs Realisasi dengan tanggal input"""
    print("=" * 80)
    print("🚀 ANALISIS PERBANDINGAN ERDKK vs REALISASI - VERSI 5 (DENGAN TANGGAL INPUT)")
    print("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # Load credentials
        print("\n🔐 Memuat credentials...")
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
        print("✅ Berhasil terhubung ke Google API")
        
        # Test koneksi spreadsheet
        try:
            spreadsheet = safe_google_api_operation(gc.open_by_url, OUTPUT_SHEET_URL)
            print(f"✅ Berhasil membuka spreadsheet: {spreadsheet.title}")
        except Exception as e:
            print(f"❌ Gagal membuka spreadsheet: {e}")
            raise
        
        # Variabel untuk cleanup
        temp_folders = []
        
        # ============================================
        # BAGIAN 1: DOWNLOAD DAN PROSES DATA ERDKK
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 1: PROSES DATA ERDKK")
        print("=" * 80)
        
        # Download file ERDKK
        erdkk_files = download_excel_files_from_drive(credentials, ERDKK_FOLDER_ID, "erdkk")
        if erdkk_files:
            temp_folders.append(erdkk_files[0]['temp_folder'] if erdkk_files else None)
        
        if not erdkk_files:
            print("⚠️  Tidak ada file ERDKK yang ditemukan")
            erdkk_kec_df = pd.DataFrame()
            erdkk_kios_df = pd.DataFrame()
            all_erdkk_rows = []
        else:
            print(f"✅ Download selesai: {len(erdkk_files)} file")
            
            # Process setiap file ERDKK
            print("\n🔄 Memproses data ERDKK...")
            all_erdkk_rows = []
            processed_files = 0
            
            for file_info in erdkk_files:
                print(f"\n📄 Processing file {processed_files + 1}/{len(erdkk_files)}")
                file_rows = process_erdkk_file(file_info['path'], file_info['name'])
                
                if file_rows:
                    all_erdkk_rows.extend(file_rows)
                    processed_files += 1
                    print(f"   ✅ File '{file_info['name']}' berhasil diproses: {len(file_rows)} baris")
                else:
                    print(f"   ⚠️  File '{file_info['name']}' tidak menghasilkan data")
            
            if all_erdkk_rows:
                print(f"\n✅ Total file ERDKK diproses: {processed_files}/{len(erdkk_files)}")
                print(f"✅ Total baris data ERDKK: {len(all_erdkk_rows)}")
                
                # Agregasi data ERDKK
                print("\n📊 Melakukan agregasi data ERDKK...")
                erdkk_kec_df = aggregate_erdkk_by_kecamatan(all_erdkk_rows)
                erdkk_kios_df = aggregate_erdkk_by_kios(all_erdkk_rows)
            else:
                print("⚠️  Tidak ada data ERDKK yang berhasil diproses")
                erdkk_kec_df = pd.DataFrame()
                erdkk_kios_df = pd.DataFrame()
        
        # ============================================
        # BAGIAN 2: DOWNLOAD DAN PROSES DATA REALISASI DENGAN TANGGAL INPUT
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 2: PROSES DATA REALISASI DENGAN TANGGAL INPUT")
        print("=" * 80)
        
        # Download file Realisasi
        realisasi_files = download_excel_files_from_drive(credentials, REALISASI_FOLDER_ID, "realisasi")
        if realisasi_files:
            temp_folders.append(realisasi_files[0]['temp_folder'] if realisasi_files else None)
        
        if not realisasi_files:
            print("⚠️  Tidak ada file realisasi yang ditemukan")
            realisasi_kec_all = pd.DataFrame()
            realisasi_kec_acc = pd.DataFrame()
            realisasi_kios_all = pd.DataFrame()
            realisasi_kios_acc = pd.DataFrame()
            all_realisasi_rows = []
            latest_tanggal_input = None
        else:
            print(f"✅ Download selesai: {len(realisasi_files)} file")
            
            # Process setiap file Realisasi
            print("\n🔄 Memproses data Realisasi...")
            all_realisasi_rows = []
            processed_files = 0
            latest_tanggal_input = None
            
            for file_info in realisasi_files:
                print(f"\n📄 Processing file {processed_files + 1}/{len(realisasi_files)}")
                file_rows = process_realisasi_file(file_info['path'], file_info['name'])
                
                if file_rows:
                    all_realisasi_rows.extend(file_rows)
                    processed_files += 1
                    print(f"   ✅ File '{file_info['name']}' berhasil diproses: {len(file_rows)} baris")
                    
                    # Cek tanggal input terbaru dari file ini
                    for row in file_rows:
                        if 'TANGGAL_INPUT' in row and row['TANGGAL_INPUT']:
                            if latest_tanggal_input is None or row['TANGGAL_INPUT'] > latest_tanggal_input:
                                latest_tanggal_input = row['TANGGAL_INPUT']
                else:
                    print(f"   ⚠️  File '{file_info['name']}' tidak menghasilkan data")
            
            if all_realisasi_rows:
                print(f"\n✅ Total file realisasi diproses: {processed_files}/{len(realisasi_files)}")
                print(f"✅ Total baris data realisasi: {len(all_realisasi_rows)}")
                
                # Tampilkan informasi tanggal input terbaru
                if latest_tanggal_input:
                    print(f"📅 TANGGAL INPUT TERBARU DARI DATA REALISASI:")
                    print(f"   • Tanggal: {latest_tanggal_input.strftime('%d %b %Y')}")
                    print(f"   • Jam: {latest_tanggal_input.strftime('%H:%M:%S')}")
                    print(f"   • Format lengkap: {latest_tanggal_input.strftime('%d %b %Y %H:%M:%S')}")
                else:
                    print(f"⚠️  Tidak dapat menentukan tanggal input terbaru dari data realisasi")
                
                # Analisis status
                df_status = pd.DataFrame(all_realisasi_rows)
                if 'STATUS' in df_status.columns:
                    print_status_analysis(df_status)
                    
                    # Cek berapa banyak yang ACC PUSAT
                    acc_pusat_count = df_status['STATUS'].apply(is_status_disetujui_pusat).sum()
                    print(f"\n📊 Status ACC PUSAT: {acc_pusat_count} baris ({acc_pusat_count/len(df_status)*100:.1f}%)")
                else:
                    print(f"⚠️  Kolom STATUS tidak ditemukan dalam data realisasi")
                
                # Agregasi data Realisasi (ALL dan ACC PUSAT)
                print("\n📊 Mengagregasi data Realisasi...")
                realisasi_kec_all = aggregate_realisasi_by_kecamatan(all_realisasi_rows, filter_acc_pusat=False)
                realisasi_kec_acc = aggregate_realisasi_by_kecamatan(all_realisasi_rows, filter_acc_pusat=True)
                realisasi_kios_all = aggregate_realisasi_by_kios(all_realisasi_rows, filter_acc_pusat=False)
                realisasi_kios_acc = aggregate_realisasi_by_kios(all_realisasi_rows, filter_acc_pusat=True)
            else:
                print("⚠️  Tidak ada data realisasi yang berhasil diproses")
                realisasi_kec_all = pd.DataFrame()
                realisasi_kec_acc = pd.DataFrame()
                realisasi_kios_all = pd.DataFrame()
                realisasi_kios_acc = pd.DataFrame()
                latest_tanggal_input = None
        
        # ============================================
        # BAGIAN 3: BUAT PERBANDINGAN
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 3: MEMBUAT PERBANDINGAN ERDKK vs REALISASI")
        print("=" * 80)
        
        if erdkk_kec_df.empty:
            print("⚠️  Data ERDKK kecamatan kosong")
            print("ℹ️  Coba membuat perbandingan dari data mentah...")
            
            # Jika data ERDKK kosong, kita tidak bisa membuat perbandingan
            kecamatan_all = pd.DataFrame()
            kecamatan_acc = pd.DataFrame()
            kios_all = pd.DataFrame()
            kios_acc = pd.DataFrame()
            success_count = 0
        else:
            print(f"✅ Data ERDKK tersedia: {len(erdkk_kec_df)} kecamatan")
            
            # Buat perbandingan untuk kecamatan
            print("\n🔍 Membuat perbandingan KECAMATAN...")
            kecamatan_all, kecamatan_acc = create_comparison_kecamatan(
                erdkk_kec_df, realisasi_kec_all, realisasi_kec_acc, latest_tanggal_input
            )
            
            # Buat perbandingan untuk kios
            print("\n🔍 Membuat perbandingan KIOS...")
            kios_all, kios_acc = create_comparison_kios(
                erdkk_kios_df, realisasi_kios_all, realisasi_kios_acc
            )
            
            # ============================================
            # BAGIAN 4: EXPORT KE GOOGLE SHEETS DENGAN TANGGAL INPUT
            # ============================================
            print("\n" + "=" * 80)
            print("📋 BAGIAN 4: EXPORT KE GOOGLE SHEETS DENGAN TANGGAL INPUT")
            print("=" * 80)
            
            print(f"\n📤 Target spreadsheet: {OUTPUT_SHEET_URL}")
            if latest_tanggal_input:
                print(f"📅 Informasi tanggal yang akan ditambahkan di Sheet1:")
                print(f"   • E1: 'Update per tanggal input'")
                print(f"   • E2: {latest_tanggal_input.strftime('%d %b %Y')}")
                print(f"   • E3: {latest_tanggal_input.strftime('%H:%M:%S')}")
            
            # Update 4 sheet yang berbeda
            updates = []
            
            # Sheet 1: kecamatan_all (dengan tanggal)
            if not kecamatan_all.empty:
                updates.append(("kecamatan_all", kecamatan_all))
                print(f"   ✅ kecamatan_all: {len(kecamatan_all)} baris")
            else:
                # Buat sheet kosong dengan pesan
                empty_df = pd.DataFrame([{
                    'KECAMATAN': 'TIDAK ADA DATA',
                    'PESAN': 'Tidak ada data ERDKK yang valid untuk perbandingan'
                }])
                updates.append(("kecamatan_all", empty_df))
                print(f"   ⚠️  kecamatan_all: Sheet kosong dibuat")
            
            # Sheet 2: kecamatan_acc_pusat
            if not kecamatan_acc.empty:
                updates.append(("kecamatan_acc_pusat", kecamatan_acc))
                print(f"   ✅ kecamatan_acc_pusat: {len(kecamatan_acc)} baris")
            else:
                # Jika kecamatan_acc kosong, buat dari kecamatan_all dengan realisasi = 0
                if not kecamatan_all.empty and 'KECAMATAN' in kecamatan_all.columns:
                    # Buat DataFrame dengan realisasi ACC PUSAT = 0
                    kecamatan_acc_empty = kecamatan_all.copy()
                    for col in kecamatan_acc_empty.columns:
                        if 'REALISASI' in col:
                            kecamatan_acc_empty[col] = 0
                        elif '%' in col:
                            kecamatan_acc_empty[col] = 0
                        elif 'SELISIH' in col:
                            # Selisih = ERDKK (karena realisasi = 0)
                            erdkk_col = col.replace('SELISIH', 'ERDKK')
                            if erdkk_col in kecamatan_acc_empty.columns:
                                kecamatan_acc_empty[col] = kecamatan_acc_empty[erdkk_col]
                    
                    updates.append(("kecamatan_acc_pusat", kecamatan_acc_empty))
                    print(f"   ℹ️  kecamatan_acc_pusat: Sheet dibuat dengan realisasi ACC PUSAT = 0")
                else:
                    empty_df = pd.DataFrame([{
                        'KECAMATAN': 'TIDAK ADA DATA ACC PUSAT',
                        'PESAN': 'Tidak ada data realisasi dengan status ACC PUSAT'
                    }])
                    updates.append(("kecamatan_acc_pusat", empty_df))
                    print(f"   ⚠️  kecamatan_acc_pusat: Sheet kosong dibuat")
            
            # Sheet 3: kios_all
            if not kios_all.empty:
                updates.append(("kios_all", kios_all))
                print(f"   ✅ kios_all: {len(kios_all)} baris")
            else:
                empty_df = pd.DataFrame([{
                    'KECAMATAN': 'TIDAK ADA DATA',
                    'KODE_KIOS': 'N/A',
                    'NAMA_KIOS': 'Tidak ada data kios yang valid untuk perbandingan'
                }])
                updates.append(("kios_all", empty_df))
                print(f"   ⚠️  kios_all: Sheet kosong dibuat")
            
            # Sheet 4: kios_acc_pusat
            if not kios_acc.empty:
                updates.append(("kios_acc_pusat", kios_acc))
                print(f"   ✅ kios_acc_pusat: {len(kios_acc)} baris")
            else:
                # Jika kios_acc kosong, buat dari kios_all dengan realisasi = 0
                if not kios_all.empty:
                    # Buat DataFrame dengan realisasi ACC PUSAT = 0
                    kios_acc_empty = kios_all.copy()
                    for col in kios_acc_empty.columns:
                        if 'REALISASI' in col:
                            kios_acc_empty[col] = 0
                        elif '%' in col:
                            kios_acc_empty[col] = 0
                        elif 'SELISIH' in col:
                            # Selisih = ERDKK (karena realisasi = 0)
                            erdkk_col = col.replace('SELISIH', 'ERDKK')
                            if erdkk_col in kios_acc_empty.columns:
                                kios_acc_empty[col] = kios_acc_empty[erdkk_col]
                    
                    updates.append(("kios_acc_pusat", kios_acc_empty))
                    print(f"   ℹ️  kios_acc_pusat: Sheet dibuat dengan realisasi ACC PUSAT = 0")
                else:
                    empty_df = pd.DataFrame([{
                        'KECAMATAN': 'TIDAK ADA DATA ACC PUSAT',
                        'KODE_KIOS': 'N/A',
                        'NAMA_KIOS': 'Tidak ada data kios dengan status ACC PUSAT'
                    }])
                    updates.append(("kios_acc_pusat", empty_df))
                    print(f"   ⚠️  kios_acc_pusat: Sheet kosong dibuat")
            
            if updates:
                success_count = batch_update_worksheets_with_date(spreadsheet, updates, latest_tanggal_input)
            else:
                print("⚠️  Tidak ada data untuk di-export")
                success_count = 0
        
        # ============================================
        # BAGIAN 5: CLEANUP TEMPORARY FILES
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 5: CLEANUP TEMPORARY FILES")
        print("=" * 80)
        
        for folder in temp_folders:
            if folder and os.path.exists(folder):
                try:
                    # Hapus semua file di folder
                    for filename in os.listdir(folder):
                        file_path = os.path.join(folder, filename)
                        try:
                            if os.path.isfile(file_path):
                                os.unlink(file_path)
                        except Exception as e:
                            print(f"   ⚠️  Gagal menghapus {file_path}: {e}")
                    
                    # Hapus folder itu sendiri
                    os.rmdir(folder)
                    print(f"✅ Folder temporary dihapus: {folder}")
                except Exception as e:
                    print(f"⚠️  Gagal menghapus folder {folder}: {e}")
        
        # ============================================
        # BAGIAN 6: SUMMARY DAN EMAIL
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 6: SUMMARY HASIL")
        print("=" * 80)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Buat summary
        total_erdkk_rows = len(all_erdkk_rows) if 'all_erdkk_rows' in locals() else 0
        total_realisasi_rows = len(all_realisasi_rows) if 'all_realisasi_rows' in locals() else 0
        
        # Hitung ACC PUSAT
        acc_pusat_count = 0
        if 'all_realisasi_rows' in locals() and all_realisasi_rows:
            df_status = pd.DataFrame(all_realisasi_rows)
            if 'STATUS' in df_status.columns:
                acc_pusat_count = df_status['STATUS'].apply(is_status_disetujui_pusat).sum()
        
        # Hitung statistik pupuk
        total_erdkk_urea = erdkk_kec_df['TOTAL_UREA'].sum() if not erdkk_kec_df.empty else 0
        total_realisasi_urea = realisasi_kec_all['REALISASI_UREA'].sum() if not realisasi_kec_all.empty else 0
        percentage_urea = (total_realisasi_urea / total_erdkk_urea * 100) if total_erdkk_urea > 0 else 0
        
        # Format informasi tanggal untuk summary
        tanggal_info = ""
        if latest_tanggal_input:
            tanggal_info = f"""
📅 INFORMASI TANGGAL INPUT REALISASI:
- Tanggal terbaru: {latest_tanggal_input.strftime('%d %b %Y')}
- Jam terbaru: {latest_tanggal_input.strftime('%H:%M:%S')}
- Informasi ditampilkan di Sheet1 (kecamatan_all) kolom E1-E3
"""
        else:
            tanggal_info = "📅 INFORMASI TANGGAL INPUT: Tidak dapat menentukan tanggal input dari data realisasi"
        
        summary_message = f"""
ANALISIS PERBANDINGAN ERDKK vs REALISASI - VERSI 5 (DENGAN TANGGAL INPUT)

⏰ Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s
📅 Tanggal analisis: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed_v5.py

{tanggal_info}

📊 DATA YANG DIPROSES:
- File ERDKK: {len(erdkk_files) if 'erdkk_files' in locals() else 0} file
- File Realisasi: {len(realisasi_files) if 'realisasi_files' in locals() else 0} file

📊 STATISTIK DATA:
- Total data ERDKK: {total_erdkk_rows} baris
- Total data Realisasi: {total_realisasi_rows} baris
- Data Realisasi ACC PUSAT: {acc_pusat_count} baris

📊 STATISTIK PUPUK (TOTAL):
- Total UREA ERDKK: {total_erdkk_urea:,.2f} Kg
- Total UREA REALISASI: {total_realisasi_urea:,.2f} Kg
- Persentase Realisasi/ERDKK: {percentage_urea:.2f}%

📋 SHEET YANG DIBUAT:
1. kecamatan_all: Perbandingan ERDKK vs Realisasi (semua status)
   • {('✅ DIBUAT' if 'kecamatan_all' in locals() and not kecamatan_all.empty else '⚠️ KOSONG')}
   • {('📅 Dengan informasi tanggal input' if latest_tanggal_input else '⚠️ Tanpa informasi tanggal')}
   
2. kecamatan_acc_pusat: Perbandingan ERDKK vs Realisasi ACC PUSAT saja
   • {('✅ DIBUAT' if 'kecamatan_acc' in locals() and not kecamatan_acc.empty else '✅ DIBUAT (KOSONG)' + ' - Tidak ada data ACC PUSAT')}
   • Kriteria ACC PUSAT: mengandung 'disetujui' dan 'pusat', TIDAK mengandung 'menunggu' atau 'ditolak'

3. kios_all: Perbandingan per Kios (semua status)
   • {('✅ DIBUAT' if 'kios_all' in locals() and not kios_all.empty else '⚠️ KOSONG')}

4. kios_acc_pusat: Perbandingan per Kios (ACC PUSAT saja)
   • {('✅ DIBUAT' if 'kios_acc' in locals() and not kios_acc.empty else '✅ DIBUAT (KOSONG)' + ' - Tidak ada data ACC PUSAT')}

🎯 FITUR BARU:
1. Ekstraksi tanggal input terbaru dari data realisasi
2. Menampilkan informasi tanggal di Sheet1 (kecamatan_all) kolom E1-E3
3. Format: E1="Update per tanggal input", E2=Tanggal (02 Jan 2026), E3=Jam (21:40:24)
4. Improved parsing tanggal dari berbagai format

📤 OUTPUT:
Spreadsheet: {OUTPUT_SHEET_URL}

✅ PROSES SELESAI: {success_count}/4 sheet berhasil diupdate
"""
        
        subject = "ANALISIS ERDKK vs REALISASI V5 " + ("BERHASIL" if success_count > 0 else "DENGAN KENDALA")
        send_email_notification(subject, summary_message, is_success=(success_count > 0))
        
        print(f"\n{'✅ ANALISIS SELESAI! 🎉' if success_count > 0 else '⚠️ ANALISIS SELESAI DENGAN KENDALA'}")
        print(f"📋 Silakan cek file: {OUTPUT_SHEET_URL}")
        print(f"   • {success_count}/4 sheet berhasil diupdate")
        print(f"   ⏰ Waktu total: {duration.seconds // 60}m {duration.seconds % 60}s")
        
        # Tampilkan statistik akhir
        if not erdkk_kec_df.empty:
            print(f"\n📊 STATISTIK AKHIR:")
            print(f"   • Jumlah kecamatan: {len(erdkk_kec_df)}")
            print(f"   • Total UREA ERDKK: {total_erdkk_urea:,.2f} Kg")
            print(f"   • Total UREA REALISASI: {total_realisasi_urea:,.2f} Kg")
            print(f"   • Persentase: {percentage_urea:.2f}%")
        
        if latest_tanggal_input:
            print(f"\n📅 INFORMASI TANGGAL INPUT:")
            print(f"   • Ditampilkan di Sheet1 (kecamatan_all) kolom E1-E3")
            print(f"   • Tanggal: {latest_tanggal_input.strftime('%d %b %Y')}")
            print(f"   • Jam: {latest_tanggal_input.strftime('%H:%M:%S')}")
        
        return success_count > 0

    except Exception as e:
        error_message = f"""
ANALISIS PERBANDINGAN ERDKK vs REALISASI GAGAL ❌

📅 Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed_v5.py
⚠️ Error: {str(e)}

🔧 Traceback:
{traceback.format_exc()}
"""
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()
        send_email_notification("ANALISIS DATA GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN SCRIPT
# ============================
if __name__ == "__main__":
    # Tambahkan error handling global
    try:
        success = process_erdkk_vs_realisasi_with_date()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Script dihentikan oleh pengguna")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR TIDAK TERDUGA: {e}")
        traceback.print_exc()
        sys.exit(1)
