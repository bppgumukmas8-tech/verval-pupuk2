"""
erdkk_vs_realisasi_fixed.py
Script untuk analisis perbandingan data ERDKK vs Realisasi Penebusan Pupuk.
VERSI DIPERBAIKI - Fix identifikasi kolom dan masalah data.

Lokasi: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py
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
from datetime import datetime
import traceback
import json
import time
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import tempfile

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
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py</small></p>
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py</small></p>
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
    """Membersihkan NIK dari karakter non-angka - DIPERBAIKI"""
    if pd.isna(nik_value) or nik_value is None:
        return None
    
    nik_str = str(nik_value).strip()
    
    # Skip jika NIK adalah 0 atau kosong
    if nik_str in ['0', 'nan', 'None', '']:
        return None
    
    cleaned_nik = re.sub(r'\D', '', nik_str)
    
    # Validasi panjang NIK
    if len(cleaned_nik) != 16:
        # Coba cari 16 digit dalam string yang lebih panjang
        if len(cleaned_nik) > 16:
            # Mungkin ada spasi atau karakter lain di tengah
            # Cari semua urutan 16 digit
            sequences = re.findall(r'\d{16}', nik_str)
            if sequences:
                cleaned_nik = sequences[0]
                print(f"   ⚠️  Diperbaiki: '{nik_value}' -> '{cleaned_nik}'")
            else:
                print(f"   ⚠️  NIK tidak standar: '{nik_value}' (panjang: {len(cleaned_nik)})")
                return None
        else:
            print(f"   ⚠️  NIK terlalu pendek: '{nik_value}' (panjang: {len(cleaned_nik)})")
            return None
    
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
# FUNGSI DEBUG FOLDER
# ============================
def debug_folder_contents(credentials, folder_id, folder_name):
    """Debug isi folder"""
    print(f"\n🔍 DEBUG: Mengecek isi folder {folder_name}...")
    
    try:
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Query untuk semua file di folder
        query = f"'{folder_id}' in parents"
        results = drive_service.files().list(
            q=query, 
            fields="files(id, name, mimeType, size)",
            pageSize=20
        ).execute()
        files = results.get("files", [])
        
        print(f"📁 Isi folder {folder_name} ({folder_id}):")
        if not files:
            print("   ⚠️  Folder kosong")
        else:
            for i, file in enumerate(files, 1):
                size_mb = int(file.get('size', 0)) / (1024*1024) if file.get('size') else 0
                print(f"   {i:2d}. {file['name']} ({file['mimeType']}) - {size_mb:.1f} MB")
        
        # Cek khusus file Excel
        excel_query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel' or mimeType='application/vnd.google-apps.spreadsheet')"
        excel_results = drive_service.files().list(
            q=excel_query, 
            fields="files(id, name, mimeType)",
            pageSize=20
        ).execute()
        excel_files = excel_results.get("files", [])
        
        print(f"\n🔍 File Excel/Spreadsheet yang terdeteksi: {len(excel_files)} file")
        for i, file in enumerate(excel_files, 1):
            print(f"   ✅ {i:2d}. {file['name']} ({file['mimeType']})")
            
        return len(files)
            
    except Exception as e:
        print(f"❌ Error debug folder {folder_name}: {e}")
        return 0

# ============================
# FUNGSI DOWNLOAD FILE
# ============================
def download_excel_files_from_drive(credentials, folder_id, folder_name):
    """Download file Excel dari Google Drive - DIPERBAIKI"""
    print(f"\n📥 Download file dari folder: {folder_name}")
    
    # Buat temporary folder
    temp_dir = tempfile.gettempdir()
    save_folder = os.path.join(temp_dir, f"data_{folder_name}_{int(time.time())}")
    os.makedirs(save_folder, exist_ok=True)
    
    try:
        drive_service = build('drive', 'v3', credentials=credentials)

        # Query untuk mencari file Excel - DIPERBAIKI mencakup lebih banyak tipe
        query = f"""
        '{folder_id}' in parents and 
        (
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or
            mimeType='application/vnd.ms-excel' or
            mimeType='application/vnd.google-apps.spreadsheet' or
            name contains '.xlsx' or
            name contains '.xls'
        )
        """
        
        print(f"   🔍 Query: {query}")
        results = drive_service.files().list(
            q=query, 
            fields="files(id, name, mimeType)",
            pageSize=10
        ).execute()
        
        files = results.get("files", [])

        if not files:
            print(f"⚠️  Tidak ada file Excel di folder {folder_name}")
            print(f"   ℹ️  Folder ID: {folder_id}")
            return []

        print(f"✅ Ditemukan {len(files)} file di folder {folder_name}")
        file_paths = []
        
        for file in files:
            print(f"   📥 Downloading: {file['name']} ({file['mimeType']})")
            
            try:
                # Untuk Google Sheets, export sebagai Excel
                if file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                    request = drive_service.files().export_media(
                        fileId=file["id"],
                        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                    ext = '.xlsx'
                else:
                    request = drive_service.files().get_media(fileId=file["id"])
                    ext = '.xlsx' if '.xlsx' in file['name'].lower() else '.xls'
                
                # Gunakan nama file yang aman
                safe_filename = re.sub(r'[^\w\s.-]', '', file['name'])
                safe_filename = safe_filename.replace(' ', '_')
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
                    'mime_type': file['mimeType']
                })
                
                print(f"      ✅ Berhasil download: {os.path.basename(file_path)}")

            except Exception as e:
                print(f"      ❌ Gagal download {file['name']}: {str(e)}")
                continue

        print(f"✅ Berhasil download {len(file_paths)}/{len(files)} file dari {folder_name}")
        return file_paths

    except Exception as e:
        print(f"❌ Error download dari {folder_name}: {str(e)}")
        traceback.print_exc()
        return []

# ============================
# FUNGSI PROSES DATA ERDKK - VERSI DIPERBAIKI
# ============================
def process_erdkk_file(file_path, file_name):
    """Proses satu file ERDKK - DIPERBAIKI dengan deteksi kolom yang lebih baik"""
    try:
        print(f"\n   📖 Memproses ERDKK: {file_name}")
        print(f"   📂 File path: {file_path}")

        # Coba berbagai sheet name yang mungkin
        sheet_names = ['Worksheet', 'Sheet1', 'Data', 'ERDKK', 'Laporan', 'Sheet']
        df = None
        
        for sheet in sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
                print(f"   ✅ Membaca sheet: {sheet}")
                break
            except Exception as e:
                continue
        
        if df is None:
            # Coba semua sheet
            try:
                xls = pd.ExcelFile(file_path)
                print(f"   📋 Sheet yang tersedia: {xls.sheet_names}")
                # Coba sheet pertama
                df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                print(f"   ✅ Membaca sheet pertama: {xls.sheet_names[0]}")
            except Exception as e:
                print(f"   ❌ Gagal membaca file: {e}")
                return []

        # Standardize column names - lebih fleksibel
        df.columns = [clean_column_name(col) for col in df.columns]
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada ({len(df.columns)} kolom):")
        for i, col in enumerate(df.columns[:30]):  # Tampilkan 30 kolom pertama
            print(f"      {i+1:2d}. {col}")
        
        if len(df.columns) > 30:
            print(f"      ... dan {len(df.columns) - 30} kolom lainnya")
        
        # Cari kolom KTP/NIK dengan pendekatan yang lebih baik
        ktp_cols = []
        for col in df.columns:
            col_upper = col.upper()
            if any(pattern in col_upper for pattern in ['NIK', 'KTP', 'NOMOR INDUK', 'NOMOR KTP']):
                ktp_cols.append(col)
        
        if ktp_cols:
            ktp_col = ktp_cols[0]
            print(f"   🔍 Kolom KTP/NIK terdeteksi: {ktp_col}")
            # Debug beberapa nilai NIK
            if len(df) > 0:
                sample_niks = df[ktp_col].head(5).tolist()
                print(f"   🔍 Sample NIK (5 pertama): {sample_niks}")
        else:
            print(f"   ⚠️  Kolom KTP/NIK tidak ditemukan, mencoba deteksi otomatis...")
            # Coba cari kolom yang berisi 16 digit angka
            for col in df.columns:
                if len(df) > 0:
                    sample = df[col].head(10).astype(str).str.strip()
                    # Cek jika ada yang seperti NIK (16 digit)
                    nik_count = sample.str.match(r'^\d{16}$').sum()
                    if nik_count > 0:
                        ktp_col = col
                        print(f"   🔍 Kolom '{col}' terdeteksi mungkin NIK ({nik_count}/10 sample valid)")
                        break
            else:
                print(f"   ❌ Kolom KTP/NIK tidak dapat diidentifikasi")
                return []
        
        # Cari kolom Nama Petani
        nama_cols = [col for col in df.columns if 'NAMA' in col and 'PETANI' in col]
        if nama_cols:
            nama_col = nama_cols[0]
        else:
            nama_cols = [col for col in df.columns if 'NAMA' in col]
            if nama_cols:
                nama_col = nama_cols[0]
            else:
                nama_col = None
        print(f"   🔍 Kolom Nama: {nama_col}")
        
        # Cari kolom Kecamatan
        kec_cols = [col for col in df.columns if 'KECAMATAN' in col]
        if kec_cols:
            kec_col = kec_cols[0]
        else:
            kec_col = None
        print(f"   🔍 Kolom Kecamatan: {kec_col}")
        
        # Cari kolom Kode Kios
        kode_kios_cols = [col for col in df.columns if 'KODE' in col and 'KIOS' in col]
        if kode_kios_cols:
            kode_kios_col = kode_kios_cols[0]
        else:
            kode_kios_col = None
        print(f"   🔍 Kolom Kode Kios: {kode_kios_col}")
        
        # Cari kolom Nama Kios
        nama_kios_cols = [col for col in df.columns if 'NAMA' in col and 'KIOS' in col]
        if nama_kios_cols:
            nama_kios_col = nama_kios_cols[0]
        else:
            nama_kios_col = None
        print(f"   🔍 Kolom Nama Kios: {nama_kios_col}")
        
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
        for pupuk_type, cols in pupuk_columns.items():
            if cols:
                print(f"   ✅ {pupuk_type}: {len(cols)} kolom")
                if len(cols) <= 5:
                    for col in cols:
                        print(f"      - {col}")
            else:
                print(f"   ⚠️  {pupuk_type}: Tidak ditemukan")
        
        # ============================================
        # PROSES SETIAP BARIS
        # ============================================
        results = []
        skipped_rows = 0
        
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik_value = row.get(ktp_col, '') if ktp_col else ''
                nik = clean_nik(nik_value)
                
                if not nik:
                    skipped_rows += 1
                    continue
                
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row.get(nama_col, '')).strip() if nama_col and pd.notna(row.get(nama_col)) else '',
                    'KECAMATAN': str(row.get(kec_col, '')).strip().upper() if kec_col and pd.notna(row.get(kec_col)) else '',
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
                
                # Hitung total per jenis pupuk
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
                                    # Bersihkan string
                                    clean_str = re.sub(r'[^\d.-]', '', value)
                                    if clean_str:
                                        num_value = float(clean_str)
                                    else:
                                        num_value = 0
                                else:
                                    num_value = 0
                                
                                total += num_value
                            except (ValueError, TypeError) as e:
                                if idx < 3:  # Print error hanya untuk 3 baris pertama
                                    print(f"      ⚠️  Baris {idx}, kolom {col}: '{value}' tidak bisa dikonversi ke angka")
                                continue
                    
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
                
            except Exception as e:
                if idx < 5:  # Print error hanya untuk 5 baris pertama
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
    
    # Pastikan KECAMATAN tidak null
    df = df[df['KECAMATAN'].notna() & (df['KECAMATAN'] != '')]
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN yang valid")
        return pd.DataFrame()
    
    print(f"   Data untuk agregasi: {len(df)} baris")
    print(f"   Jumlah kecamatan unik: {df['KECAMATAN'].nunique()}")
    
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
    
    print(f"✅ Agregasi kecamatan selesai: {len(kec_df)} kecamatan")
    
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
    
    print(f"   Data untuk agregasi kios: {len(df)} baris")
    print(f"   Jumlah kios unik: {df['KODE_KIOS'].nunique()}")
    
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
    
    print(f"✅ Agregasi kios selesai: {len(kios_df)} kios")
    
    if len(kios_df) > 0:
        print(f"\n📊 Sample agregasi kios (3 pertama):")
        print(kios_df.head(3).to_string())
    
    return kios_df

# ============================
# FUNGSI PROSES DATA REALISASI - VERSI DIPERBAIKI
# ============================
def process_realisasi_file(file_path, file_name):
    """Proses satu file realisasi - DIPERBAIKI untuk identifikasi kolom yang benar"""
    try:
        print(f"\n   📖 Memproses Realisasi: {file_name}")
        print(f"   📂 File path: {file_path}")

        # Baca file Excel
        try:
            df = pd.read_excel(file_path, dtype=str)  # Baca semua sebagai string dulu
        except Exception as e:
            print(f"   ❌ Gagal membaca file: {e}")
            return []
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada ({len(df.columns)} kolom):")
        for i, col in enumerate(df.columns):
            print(f"      {i+1:2d}. '{col}'")
        
        # DEBUG: Tampilkan beberapa baris pertama untuk analisis
        if len(df) > 0:
            print(f"\n   🔍 DEBUG - 3 baris pertama data:")
            for i in range(min(3, len(df))):
                row_data = []
                for col in df.columns[:10]:  # Tampilkan 10 kolom pertama
                    value = df.iloc[i][col]
                    row_data.append(f"{col[:15]}: '{str(value)[:20]}'")
                print(f"      Baris {i+1}: {' | '.join(row_data)}")
        
        # IDENTIFIKASI KOLOM NIK - DIPERBAIKI
        nik_col = None
        
        # Pola pencarian spesifik untuk NIK
        nik_patterns = [
            r'^NIK$',                    # Persis "NIK"
            r'^NIK\s',                   # Dimulai dengan "NIK"
            r'\sNIK$',                   # Diakhiri dengan "NIK"
            r'^KTP$',                    # Persis "KTP"
            r'^KTP\s',                   # Dimulai dengan "KTP"
            r'\sKTP$',                   # Diakhiri dengan "KTP"
            r'NOMOR\s+INDUK',           # Nama lengkap
            r'NOMOR\s+KTP',             # Nomor KTP
            r'NO\.?\s*KTP',             # No. KTP
            r'NO\.?\s*NIK',             # No. NIK
        ]
        
        # Cari berdasarkan nama kolom
        for col in df.columns:
            col_upper = col.upper()
            
            # Cek pola spesifik
            for pattern in nik_patterns:
                if re.search(pattern, col_upper, re.IGNORECASE):
                    nik_col = col
                    print(f"   🔍 Kolom NIK ditemukan dengan pola '{pattern}': {col}")
                    break
            
            if nik_col:
                break
        
        # Jika belum ditemukan, cari yang mengandung kata kunci
        if not nik_col:
            for col in df.columns:
                col_upper = col.upper()
                if 'NIK' in col_upper or 'KTP' in col_upper:
                    nik_col = col
                    print(f"   🔍 Kolom NIK ditemukan dengan kata kunci: {col}")
                    break
        
        # Jika masih belum ditemukan, coba deteksi berdasarkan data
        if not nik_col:
            print(f"   ⚠️  Kolom NIK tidak ditemukan berdasarkan nama, mencoba deteksi data...")
            for col in df.columns:
                if len(df) > 0:
                    # Ambil sample data
                    sample_data = df[col].head(20).astype(str).str.strip()
                    # Cek jika ada yang seperti NIK (16 digit angka)
                    nik_count = sample_data.str.match(r'^\d{16}$').sum()
                    if nik_count >= 5:  # Minimal 5 dari 20 sample adalah NIK
                        nik_col = col
                        print(f"   🔍 Kolom '{col}' terdeteksi sebagai NIK ({nik_count}/20 sample valid)")
                        # Tampilkan sample NIK
                        valid_niks = sample_data[sample_data.str.match(r'^\d{16}$')].head(3).tolist()
                        print(f"      Sample NIK: {valid_niks}")
                        break
        
        if not nik_col:
            print(f"   ❌ ERROR: Kolom NIK tidak dapat diidentifikasi!")
            print(f"   💡 Saran: Periksa struktur file '{file_name}'")
            return []
        
        # Identifikasi kolom lainnya
        nama_col = None
        kec_col = None
        kode_kios_col = None
        nama_kios_col = None
        status_col = None
        
        for col in df.columns:
            col_upper = col.upper()
            
            # Skip kolom NIK yang sudah ditemukan
            if col == nik_col:
                continue
                
            # NAMA PETANI
            if not nama_col:
                if 'NAMA' in col_upper and 'PETANI' in col_upper:
                    nama_col = col
                elif 'NAMA' in col_upper and nama_col is None:
                    nama_col = col
            
            # KECAMATAN
            if not kec_col and 'KECAMATAN' in col_upper:
                kec_col = col
            
            # KODE KIOS
            if not kode_kios_col and 'KODE' in col_upper and 'KIOS' in col_upper:
                kode_kios_col = col
            
            # NAMA KIOS
            if not nama_kios_col and 'NAMA' in col_upper and 'KIOS' in col_upper:
                nama_kios_col = col
            
            # STATUS
            if not status_col and 'STATUS' in col_upper:
                status_col = col
        
        # Fallback untuk kode kios
        if not kode_kios_col:
            for col in df.columns:
                col_upper = col.upper()
                if 'KODE' in col_upper or 'ID' in col_upper or 'KIOS' in col_upper:
                    kode_kios_col = col
                    break
        
        print(f"\n   ✅ Kolom yang teridentifikasi:")
        print(f"     NIK: {nik_col}")
        print(f"     NAMA: {nama_col}")
        print(f"     KECAMATAN: {kec_col}")
        print(f"     KODE_KIOS: {kode_kios_col}")
        print(f"     NAMA_KIOS: {nama_kios_col}")
        print(f"     STATUS: {status_col}")
        
        # Cari kolom pupuk
        pupuk_mapping = {
            'UREA': ['UREA'],
            'NPK': ['NPK'],
            'SP36': ['SP36', 'SP-36'],
            'ZA': ['ZA'],
            'NPK_FORMULA': ['NPK.*FORMULA', 'FORMULA'],
            'ORGANIK': ['ORGANIK'],
            'ORGANIK_CAIR': ['ORGANIK.*CAIR', 'CAIR']
        }
        
        pupuk_cols = {}
        for pupuk_type, patterns in pupuk_mapping.items():
            found = False
            for pattern in patterns:
                if found:
                    break
                for col in df.columns:
                    if re.search(pattern, col, re.IGNORECASE):
                        pupuk_cols[pupuk_type] = col
                        print(f"     {pupuk_type}: {col}")
                        found = True
                        break
        
        results = []
        skipped_rows = 0
        
        # Proses setiap baris
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik_value = row.get(nik_col, '') if nik_col else ''
                nik = clean_nik(nik_value)
                
                if not nik:
                    skipped_rows += 1
                    if idx < 5:
                        print(f"   ⚠️  Baris {idx}: NIK tidak valid '{nik_value}'")
                    continue
                
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row.get(nama_col, '')).strip() if nama_col and pd.notna(row.get(nama_col)) else '',
                    'KECAMATAN': str(row.get(kec_col, '')).strip().upper() if kec_col and pd.notna(row.get(kec_col)) else '',
                    'KODE_KIOS': str(row.get(kode_kios_col, '')).strip().upper() if kode_kios_col and pd.notna(row.get(kode_kios_col)) else '',
                    'NAMA_KIOS': str(row.get(nama_kios_col, '')).strip() if nama_kios_col and pd.notna(row.get(nama_kios_col)) else '',
                    'STATUS': str(row.get(status_col, '')).strip() if status_col and pd.notna(row.get(status_col)) else '',
                    'REALISASI_UREA': 0,
                    'REALISASI_NPK': 0,
                    'REALISASI_SP36': 0,
                    'REALISASI_ZA': 0,
                    'REALISASI_NPK_FORMULA': 0,
                    'REALISASI_ORGANIK': 0,
                    'REALISASI_ORGANIK_CAIR': 0,
                    'FILE_SOURCE': file_name
                }
                
                # Hitung realisasi pupuk
                for pupuk_type, col_name in pupuk_cols.items():
                    if col_name and col_name in row:
                        value = row[col_name]
                        try:
                            if pd.isna(value):
                                num_value = 0
                            elif isinstance(value, (int, float)):
                                num_value = float(value)
                            elif isinstance(value, str):
                                clean_str = re.sub(r'[^\d.-]', '', value)
                                num_value = float(clean_str) if clean_str else 0
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
                
            except Exception as e:
                if idx < 5:  # Print error hanya untuk 5 baris pertama
                    print(f"   ⚠️  Error processing row {idx}: {e}")
                skipped_rows += 1
                continue
        
        print(f"   ✅ Berhasil diproses: {len(results)} baris data")
        if skipped_rows > 0:
            print(f"   ⚠️  Dilewati: {skipped_rows} baris (NIK tidak valid)")
        
        # Tampilkan sample
        if results:
            print(f"\n   🔍 Sample data (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     STATUS: {sample['STATUS']}")
            print(f"     KECAMATAN: {sample['KECAMATAN']}")
            print(f"     KODE_KIOS: {sample['KODE_KIOS']}")
            print(f"     UREA: {sample['REALISASI_UREA']}")
            print(f"     NPK: {sample['REALISASI_NPK']}")
            print(f"     Is ACC PUSAT? {is_status_disetujui_pusat(sample['STATUS'])}")
        
        return results

    except Exception as e:
        print(f"   ❌ Error memproses realisasi {file_name}: {str(e)}")
        traceback.print_exc()
        return []

def aggregate_realisasi_by_kecamatan(all_realisasi_rows, filter_acc_pusat=False):
    """Agregasi data realisasi per Kecamatan"""
    if not all_realisasi_rows:
        print(f"⚠️  Tidak ada data realisasi untuk diagregasi (filter: {'ACC PUSAT' if filter_acc_pusat else 'ALL'})")
        return pd.DataFrame()

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
        return pd.DataFrame()
    
    # Pastikan KECAMATAN tidak null
    df = df[df['KECAMATAN'].notna() & (df['KECAMATAN'] != '')]
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN yang valid")
        return pd.DataFrame()

    print(f"   Data untuk agregasi: {len(df)} baris")
    print(f"   Jumlah kecamatan unik: {df['KECAMATAN'].nunique()}")

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
    
    print(f"✅ Agregasi realisasi kecamatan selesai: {len(kec_df)} kecamatan")
    
    if len(kec_df) > 0:
        print(f"\n📊 Sample agregasi realisasi kecamatan:")
        print(kec_df.head(3).to_string())
    
    return kec_df

def aggregate_realisasi_by_kios(all_realisasi_rows, filter_acc_pusat=False):
    """Agregasi data realisasi per Kode Kios"""
    if not all_realisasi_rows:
        print(f"⚠️  Tidak ada data realisasi untuk diagregasi (filter: {'ACC PUSAT' if filter_acc_pusat else 'ALL'})")
        return pd.DataFrame()

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
        return pd.DataFrame()
    
    # Filter yang punya KECAMATAN dan KODE_KIOS
    mask = df['KECAMATAN'].notna() & (df['KECAMATAN'] != '') & df['KODE_KIOS'].notna() & (df['KODE_KIOS'] != '')
    df = df[mask]
    
    if df.empty:
        print("⚠️  Tidak ada data dengan KECAMATAN dan KODE_KIOS yang valid")
        return pd.DataFrame()

    print(f"   Data untuk agregasi kios: {len(df)} baris")
    print(f"   Jumlah kios unik: {df['KODE_KIOS'].nunique()}")

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
    
    print(f"✅ Agregasi realisasi kios selesai: {len(kios_df)} kios")
    
    if len(kios_df) > 0:
        print(f"\n📊 Sample agregasi realisasi kios:")
        print(kios_df.head(3).to_string())
    
    return kios_df

# ============================
# FUNGSI BUAT PERBANDINGAN
# ============================
def create_comparison_kecamatan(erdkk_kec_df, realisasi_kec_df_all, realisasi_kec_df_acc):
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
        
        # Untuk ACC PUSAT
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
    
    if len(comparison_all) > 0:
        print(f"\n📊 Struktur kolom untuk UREA (contoh):")
        urea_cols = [col for col in comparison_all.columns if 'UREA' in col]
        print(f"   {urea_cols}")
        
        print(f"\n📊 Sample data (termasuk TOTAL):")
        # Tampilkan 3 baris pertama dan baris terakhir (TOTAL)
        if len(comparison_all) > 3:
            sample = pd.concat([comparison_all.head(3), comparison_all.tail(1)])
            print(sample[['KECAMATAN', 'UREA ERDKK', 'UREA REALISASI', 'UREA SELISIH', 'UREA %']].to_string())
    
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
        
        # Untuk ACC PUSAT
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
# FUNGSI UPDATE GOOGLE SHEETS
# ============================
def format_worksheet(worksheet, df):
    """Format worksheet dengan warna header dan border"""
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
        
        # Format header
        worksheet.format("1:1", header_format)
        
        # Format baris TOTAL
        total_row = len(df) + 1  # +1 karena header di baris 1
        worksheet.format(f"{total_row}:{total_row}", total_format)
        
        # Format kolom persentase
        for col_idx, col_name in enumerate(df.columns, start=1):
            if '%' in col_name:
                col_letter = gspread.utils.rowcol_to_a1(1, col_idx)[0]  # Ambil huruf kolom
                worksheet.format(f"{col_letter}2:{col_letter}{total_row}", percent_format)
            elif any(x in col_name for x in ['ERDKK', 'REALISASI', 'SELISIH']):
                col_letter = gspread.utils.rowcol_to_a1(1, col_idx)[0]
                worksheet.format(f"{col_letter}2:{col_letter}{total_row}", number_format)
        
        # Set lebar kolom otomatis
        worksheet.columns_auto_resize(start_column_index=0, end_column_index=len(df.columns))
        
        # Freeze header row
        worksheet.freeze(rows=1)
        
        print(f"      ✅ Formatting diterapkan")
        
    except Exception as e:
        print(f"      ⚠️  Gagal formatting: {e}")

def batch_update_worksheets(spreadsheet, updates):
    """Batch update untuk multiple worksheets dengan formatting"""
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
            
            # Format worksheet
            format_worksheet(worksheet, data)
            
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
# FUNGSI UTAMA - DIPERBAIKI
# ============================
def process_erdkk_vs_realisasi():
    """Fungsi utama untuk analisis perbandingan ERDKK vs Realisasi - DIPERBAIKI"""
    print("=" * 80)
    print("🚀 ANALISIS PERBANDINGAN ERDKK vs REALISASI (VERSI DIPERBAIKI)")
    print("=" * 80)
    print(f"📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py")
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
        
        # DEBUG: Cek isi folder ERDKK dan REALISASI
        print("\n🔍 DEBUG: Mengecek isi folder...")
        debug_folder_contents(credentials, ERDKK_FOLDER_ID, "ERDKK")
        debug_folder_contents(credentials, REALISASI_FOLDER_ID, "REALISASI")
        
        # Test koneksi spreadsheet
        try:
            spreadsheet = safe_google_api_operation(gc.open_by_url, OUTPUT_SHEET_URL)
            print(f"\n✅ Berhasil membuka spreadsheet: {spreadsheet.title}")
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
        # BAGIAN 2: DOWNLOAD DAN PROSES DATA REALISASI
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 2: PROSES DATA REALISASI")
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
        else:
            print(f"✅ Download selesai: {len(realisasi_files)} file")
            
            # Process setiap file Realisasi
            print("\n🔄 Memproses data Realisasi...")
            all_realisasi_rows = []
            processed_files = 0
            
            for file_info in realisasi_files:
                print(f"\n📄 Processing file {processed_files + 1}/{len(realisasi_files)}")
                file_rows = process_realisasi_file(file_info['path'], file_info['name'])
                
                if file_rows:
                    all_realisasi_rows.extend(file_rows)
                    processed_files += 1
                    print(f"   ✅ File '{file_info['name']}' berhasil diproses: {len(file_rows)} baris")
                else:
                    print(f"   ⚠️  File '{file_info['name']}' tidak menghasilkan data")
            
            if all_realisasi_rows:
                print(f"\n✅ Total file realisasi diproses: {processed_files}/{len(realisasi_files)}")
                print(f"✅ Total baris data realisasi: {len(all_realisasi_rows)}")
                
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
        
        # ============================================
        # BAGIAN 3: BUAT PERBANDINGAN
        # ============================================
        print("\n" + "=" * 80)
        print("📋 BAGIAN 3: MEMBUAT PERBANDINGAN ERDKK vs REALISASI")
        print("=" * 80)
        
        if not erdkk_kec_df.empty:
            print(f"✅ Data ERDKK tersedia: {len(erdkk_kec_df)} baris")
            
            # Buat perbandingan untuk kecamatan
            print("\n🔍 Membuat perbandingan KECAMATAN...")
            kecamatan_all, kecamatan_acc = create_comparison_kecamatan(
                erdkk_kec_df, realisasi_kec_all, realisasi_kec_acc
            )
            
            # Buat perbandingan untuk kios
            print("\n🔍 Membuat perbandingan KIOS...")
            kios_all, kios_acc = create_comparison_kios(
                erdkk_kios_df, realisasi_kios_all, realisasi_kios_acc
            )
            
            # ============================================
            # BAGIAN 4: EXPORT KE GOOGLE SHEETS
            # ============================================
            print("\n" + "=" * 80)
            print("📋 BAGIAN 4: EXPORT KE GOOGLE SHEETS")
            print("=" * 80)
            
            print(f"\n📤 Target spreadsheet: {OUTPUT_SHEET_URL}")
            print(f"📊 Data yang akan diexport:")
            
            updates = []
            sheet_data = [
                ("kecamatan_all", kecamatan_all),
                ("kecamatan_acc_pusat", kecamatan_acc),
                ("kios_all", kios_all),
                ("kios_acc_pusat", kios_acc)
            ]
            
            for sheet_name, data in sheet_data:
                if not data.empty:
                    print(f"   • {sheet_name}: {len(data)} baris, {len(data.columns)} kolom")
                    updates.append((sheet_name, data))
                else:
                    print(f"   ⚠️  {sheet_name}: Data kosong")
            
            if updates:
                success_count = batch_update_worksheets(spreadsheet, updates)
            else:
                print("⚠️  Tidak ada data untuk di-export")
                success_count = 0
        else:
            print("❌ Data ERDKK kosong, tidak dapat membuat perbandingan")
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
        
        summary_message = f"""
ANALISIS PERBANDINGAN ERDKK vs REALISASI (VERSI DIPERBAIKI)

⏰ Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py

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
   • {'✅ DIBUAT' if 'kecamatan_all' in locals() and not kecamatan_all.empty else '❌ TIDAK ADA DATA'}
   
2. kecamatan_acc_pusat: Perbandingan ERDKK vs Realisasi ACC PUSAT saja
   • {'✅ DIBUAT' if 'kecamatan_acc' in locals() and not kecamatan_acc.empty else '❌ TIDAK ADA DATA'}
   • Kriteria ACC PUSAT: mengandung 'disetujui' dan 'pusat', TIDAK mengandung 'menunggu' atau 'ditolak'

3. kios_all: Perbandingan per Kios (semua status)
   • {'✅ DIBUAT' if 'kios_all' in locals() and not kios_all.empty else '❌ TIDAK ADA DATA'}

4. kios_acc_pusat: Perbandingan per Kios (ACC PUSAT saja)
   • {'✅ DIBUAT' if 'kios_acc' in locals() and not kios_acc.empty else '❌ TIDAK ADA DATA'}

🎯 PERBAIKAN YANG DITERAPKAN:
1. Deteksi kolom NIK yang lebih akurat
2. Penanganan kasus NIK = 0 atau tidak valid
3. Debug detail isi folder
4. Identifikasi kolom pupuk yang lebih robust
5. Error handling yang lebih baik

📤 OUTPUT:
Spreadsheet: {OUTPUT_SHEET_URL}

✅ PROSES SELESAI: {success_count}/4 sheet berhasil diupdate
"""
        
        subject = "ANALISIS ERDKK vs REALISASI " + ("BERHASIL" if success_count > 0 else "DENGAN KENDALA")
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
        
        return success_count > 0

    except Exception as e:
        error_message = f"""
ANALISIS PERBANDINGAN ERDKK vs REALISASI GAGAL ❌

📅 Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi_fixed.py
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
        success = process_erdkk_vs_realisasi()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Script dihentikan oleh pengguna")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR TIDAK TERDUGA: {e}")
        traceback.print_exc()
        sys.exit(1)
