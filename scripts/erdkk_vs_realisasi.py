"""
erdkk_vs_realisasi.py
Script untuk analisis perbandingan data ERDKK vs Realisasi Penebusan Pupuk.

Lokasi: verval-pupuk2/scripts/erdkk_vs_realisasi.py
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi.py</small></p>
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi.py</small></p>
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
    """Membersihkan NIK dari karakter non-angka"""
    if pd.isna(nik_value) or nik_value is None:
        return None
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)
    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")
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
    
    # Buat temporary folder
    temp_dir = tempfile.gettempdir()
    save_folder = os.path.join(temp_dir, f"data_{folder_name}_{int(time.time())}")
    os.makedirs(save_folder, exist_ok=True)
    
    try:
        drive_service = build('drive', 'v3', credentials=credentials)

        # Query untuk mencari file Excel
        query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])

        if not files:
            print(f"⚠️  Tidak ada file Excel di folder {folder_name}")
            return []

        file_paths = []
        for file in files:
            print(f"   📥 Downloading: {file['name']}")
            request = drive_service.files().get_media(fileId=file["id"])
            
            # Gunakan nama file yang aman
            safe_filename = "".join(c for c in file['name'] if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            file_path = os.path.join(save_folder, safe_filename)

            with io.FileIO(file_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()

            file_paths.append({
                'path': file_path,
                'name': file['name'],
                'temp_folder': save_folder
            })

        print(f"✅ Berhasil download {len(file_paths)} file Excel dari {folder_name}")
        return file_paths

    except Exception as e:
        print(f"❌ Error download dari {folder_name}: {str(e)}")
        return []

# ============================
# FUNGSI PROSES DATA ERDKK - VERSI DIPERBAIKI
# ============================
def process_erdkk_file(file_path, file_name):
    """Proses satu file ERDKK - PERBAIKAN PERHITUNGAN AGREGAT"""
    try:
        print(f"\n   📖 Memproses ERDKK: {file_name}")

        # Coba berbagai sheet name yang mungkin
        sheet_names = ['Worksheet', 'Sheet1', 'Data', 'ERDKK', 'Laporan']
        df = None
        
        for sheet in sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet)
                print(f"   ✅ Membaca sheet: {sheet}")
                break
            except:
                continue
        
        if df is None:
            # Coba sheet pertama
            try:
                df = pd.read_excel(file_path, sheet_name=0)
                print(f"   ✅ Membaca sheet pertama (index 0)")
            except Exception as e:
                print(f"   ❌ Gagal membaca file: {e}")
                return []

        # Standardize column names - lebih fleksibel
        df.columns = df.columns.astype(str).str.strip().str.upper()
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada: {list(df.columns)[:20]}")
        
        # Cari kolom KTP/NIK
        ktp_cols = [col for col in df.columns if 'KTP' in col or 'NIK' in col]
        if ktp_cols:
            ktp_col = ktp_cols[0]
            print(f"   🔍 Kolom KTP/NIK: {ktp_col}")
        else:
            print(f"   ⚠️  Kolom KTP/NIK tidak ditemukan")
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
                nama_col = ''
        print(f"   🔍 Kolom Nama: {nama_col}")
        
        # Cari kolom Kecamatan
        kec_cols = [col for col in df.columns if 'KECAMATAN' in col]
        if kec_cols:
            kec_col = kec_cols[0]
        else:
            kec_col = ''
        print(f"   🔍 Kolom Kecamatan: {kec_col}")
        
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
        # PERBAIKAN: CARI KOLOM PUPUK YANG TEPAT
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
                        # Cek apakah ini kolom per MT atau total
                        if any(mt in col_upper for mt in ['MT1', 'MT2', 'MT3', 'MT 1', 'MT 2', 'MT 3']):
                            pupuk_columns[pupuk_type].append(col)
                        break
        
        # Tampilkan kolom yang ditemukan
        for pupuk_type, cols in pupuk_columns.items():
            if cols:
                print(f"   ✅ {pupuk_type}: {len(cols)} kolom ditemukan")
                if len(cols) <= 3:  # Tampilkan maks 3 kolom
                    print(f"      {cols}")
            else:
                print(f"   ⚠️  {pupuk_type}: Tidak ditemukan kolom")
        
        # ============================================
        # PROSES SETIAP BARIS DENGAN PERHITUNGAN YANG BENAR
        # ============================================
        results = []
        
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik = clean_nik(row.get(ktp_col, ''))
                if not nik:
                    continue
                
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row.get(nama_col, '')).strip() if nama_col else '',
                    'KECAMATAN': str(row.get(kec_col, '')).strip().upper() if kec_col else '',
                    'KODE_KIOS': str(row.get(kode_kios_col, '')).strip().upper() if kode_kios_col else '',
                    'NAMA_KIOS': str(row.get(nama_kios_col, '')).strip() if nama_kios_col else '',
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
                                num_value = float(value)
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
                
            except Exception as e:
                if idx < 10:  # Print error hanya untuk 10 baris pertama
                    print(f"   ⚠️  Error processing row {idx}: {e}")
                continue
        
        print(f"   ✅ Berhasil: {len(results)} baris data")
        
        # Tampilkan sample dengan detail
        if results:
            print(f"\n   🔍 Sample data (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     NAMA: {sample['NAMA_PETANI'][:30]}...")
            print(f"     KECAMATAN: {sample['KECAMATAN']}")
            print(f"     KODE_KIOS: {sample['KODE_KIOS']}")
            print(f"     UREA: {sample['TOTAL_UREA']:.2f} Kg")
            print(f"     NPK: {sample['TOTAL_NPK']:.2f} Kg")
            print(f"     SP36: {sample['TOTAL_SP36']:.2f} Kg")
            print(f"     ZA: {sample['TOTAL_ZA']:.2f} Kg")
            
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
# FUNGSI PROSES DATA REALISASI
# ============================
def process_realisasi_file(file_path, file_name):
    """Proses satu file realisasi"""
    try:
        print(f"\n   📖 Memproses Realisasi: {file_name}")

        # Baca file Excel
        df = pd.read_excel(file_path)
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        print(f"   📊 DataFrame shape: {df.shape}")
        print(f"   📋 Kolom yang ada: {list(df.columns)[:15]}")
        
        # Cari kolom yang diperlukan
        nik_col = ''
        nama_col = ''
        kec_col = ''
        kode_kios_col = ''
        nama_kios_col = ''
        status_col = ''
        
        # Cari kolom berdasarkan pola
        for col in df.columns:
            col_upper = col.upper()
            if 'NIK' in col_upper or 'KTP' in col_upper:
                nik_col = col
            elif 'NAMA' in col_upper and 'PETANI' in col_upper:
                nama_col = col
            elif 'KECAMATAN' in col_upper:
                kec_col = col
            elif 'KODE' in col_upper and 'KIOS' in col_upper:
                kode_kios_col = col
            elif 'NAMA' in col_upper and 'KIOS' in col_upper:
                nama_kios_col = col
            elif 'STATUS' in col_upper:
                status_col = col
        
        # Jika tidak ditemukan, coba tebak berdasarkan urutan
        if not nik_col and len(df.columns) > 0:
            nik_col = df.columns[0]
        if not nama_col and len(df.columns) > 1:
            nama_col = df.columns[1]
        if not kec_col and len(df.columns) > 2:
            kec_col = df.columns[2]
        
        print(f"   🔍 Kolom yang teridentifikasi:")
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
            for pattern in patterns:
                found_cols = [col for col in df.columns if re.search(pattern, col, re.IGNORECASE)]
                if found_cols:
                    pupuk_cols[pupuk_type] = found_cols[0]
                    print(f"     {pupuk_type}: {found_cols[0]}")
                    break
        
        results = []
        
        # Proses setiap baris
        for idx, row in df.iterrows():
            try:
                # Clean NIK
                nik = clean_nik(row.get(nik_col, '')) if nik_col else ''
                if not nik:
                    continue
                
                result = {
                    'NIK': nik,
                    'NAMA_PETANI': str(row.get(nama_col, '')).strip() if nama_col else '',
                    'KECAMATAN': str(row.get(kec_col, '')).strip().upper() if kec_col else '',
                    'KODE_KIOS': str(row.get(kode_kios_col, '')).strip().upper() if kode_kios_col else '',
                    'NAMA_KIOS': str(row.get(nama_kios_col, '')).strip() if nama_kios_col else '',
                    'STATUS': str(row.get(status_col, '')).strip() if status_col else '',
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
                            num_value = float(value) if pd.notna(value) else 0
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
                continue
        
        print(f"   ✅ Berhasil: {len(results)} baris data")
        
        # Tampilkan sample
        if results:
            print(f"\n   🔍 Sample data:")
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
        print(f"   ❌ Error memproses realisasi {file_name}: {str(str(e))[:100]}...")
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
            print(f"   📝 Processing {i+1}/{len(updates)}: {sheet_name}")
            
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
                    rows="1000", 
                    cols="50"
                )
                print(f"      ✅ Membuat sheet baru")
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
# FUNGSI UTAMA
# ============================
def process_erdkk_vs_realisasi():
    """Fungsi utama untuk analisis perbandingan ERDKK vs Realisasi"""
    print("=" * 80)
    print("🚀 ANALISIS PERBANDINGAN ERDKK vs REALISASI")
    print("=" * 80)
    print(f"📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi.py")
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
        else:
            print(f"✅ Download selesai: {len(erdkk_files)} file")
            
            # Process setiap file ERDKK
            print("\n🔄 Memproses data ERDKK...")
            all_erdkk_rows = []
            processed_files = 0
            
            for file_info in erdkk_files:
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
                
                # Verifikasi perhitungan untuk Ajung
                if not erdkk_kec_df.empty and 'AJUNG' in erdkk_kec_df['KECAMATAN'].values:
                    ajung_data = erdkk_kec_df[erdkk_kec_df['KECAMATAN'] == 'AJUNG']
                    if not ajung_data.empty:
                        urea_ajung = ajung_data.iloc[0]['TOTAL_UREA']
                        print(f"\n🔍 VERIFIKASI: Kecamatan AJUNG")
                        print(f"   • TOTAL_UREA dari script: {urea_ajung:,.2f} Kg")
                        print(f"   • TOTAL_UREA manual: 2,972,848.00 Kg")
                        print(f"   • Selisih: {urea_ajung - 2972848:,.2f} Kg")
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
        else:
            print(f"✅ Download selesai: {len(realisasi_files)} file")
            
            # Process setiap file Realisasi
            print("\n🔄 Memproses data Realisasi...")
            all_realisasi_rows = []
            processed_files = 0
            
            for file_info in realisasi_files:
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
            print(f"   • kecamatan_all: {len(kecamatan_all) if 'kecamatan_all' in locals() and not kecamatan_all.empty else 0} baris")
            print(f"   • kecamatan_acc_pusat: {len(kecamatan_acc) if 'kecamatan_acc' in locals() and not kecamatan_acc.empty else 0} baris")
            print(f"   • kios_all: {len(kios_all) if 'kios_all' in locals() and not kios_all.empty else 0} baris")
            print(f"   • kios_acc_pusat: {len(kios_acc) if 'kios_acc' in locals() and not kios_acc.empty else 0} baris")
            
            # Update 4 sheet yang berbeda
            updates = []
            if not kecamatan_all.empty:
                updates.append(("kecamatan_all", kecamatan_all))
            if not kecamatan_acc.empty:
                updates.append(("kecamatan_acc_pusat", kecamatan_acc))
            if not kios_all.empty:
                updates.append(("kios_all", kios_all))
            if not kios_acc.empty:
                updates.append(("kios_acc_pusat", kios_acc))
            
            if updates:
                success_count = batch_update_worksheets(spreadsheet, updates)
                
                # Verifikasi akhir
                print(f"\n🔍 Verifikasi spreadsheet...")
                try:
                    final_sheets = safe_google_api_operation(spreadsheet.worksheets)
                    sheet_names = [ws.title for ws in final_sheets]
                    print(f"📋 Sheets di spreadsheet setelah update: {sheet_names}")
                    
                    # Cek apakah semua sheet yang diinginkan ada
                    required_sheets = ['kecamatan_all', 'kecamatan_acc_pusat', 'kios_all', 'kios_acc_pusat']
                    missing_sheets = [sheet for sheet in required_sheets if sheet not in sheet_names]
                    
                    if missing_sheets:
                        print(f"⚠️  Sheet yang hilang: {missing_sheets}")
                    else:
                        print(f"✅ Semua 4 sheet berhasil dibuat/diupdate")
                        
                except Exception as e:
                    print(f"⚠️  Tidak bisa verifikasi spreadsheet: {e}")
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
        acc_pusat_count = len(pd.DataFrame(all_realisasi_rows)) if 'all_realisasi_rows' in locals() and all_realisasi_rows else 0
        if 'all_realisasi_rows' in locals() and all_realisasi_rows:
            df_status = pd.DataFrame(all_realisasi_rows)
            if 'STATUS' in df_status.columns:
                acc_pusat_count = df_status['STATUS'].apply(is_status_disetujui_pusat).sum()
        
        # Hitung statistik untuk summary
        total_erdkk_urea = erdkk_kec_df['TOTAL_UREA'].sum() if not erdkk_kec_df.empty else 0
        total_realisasi_urea = realisasi_kec_all['REALISASI_UREA'].sum() if not realisasi_kec_all.empty else 0
        percentage_urea = (total_realisasi_urea / total_erdkk_urea * 100) if total_erdkk_urea > 0 else 0
        
        summary_message = f"""
ANALISIS PERBANDINGAN ERDKK vs REALISASI

⏰ Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi.py

📊 DATA YANG DIPROSES:
- File ERDKK: {len(erdkk_files) if 'erdkk_files' in locals() else 0} file
- File Realisasi: {len(realisasi_files) if 'realisasi_files' in locals() else 0} file

📊 STATISTIK DATA:
- Total data ERDKK: {len(all_erdkk_rows) if 'all_erdkk_rows' in locals() else 0} baris
- Total data Realisasi: {len(all_realisasi_rows) if 'all_realisasi_rows' in locals() else 0} baris
- Data Realisasi ACC PUSAT: {acc_pusat_count} baris

📊 STATISTIK PUPUK (TOTAL):
- Total UREA ERDKK: {total_erdkk_urea:,.2f} Kg
- Total UREA REALISASI: {total_realisasi_urea:,.2f} Kg
- Persentase Realisasi/ERDKK: {percentage_urea:.2f}%

📋 SHEET YANG DIBUAT:
1. kecamatan_all: Perbandingan ERDKK vs Realisasi (semua status)
   • Struktur: KECAMATAN | [Pupuk] ERDKK | [Pupuk] REALISASI | [Pupuk] SELISIH | [Pupuk] %
   • Termasuk baris TOTAL di akhir
   • Format: Header berwarna biru, baris TOTAL abu-abu
   • Persentase ditampilkan sebagai persen (61.06% bukan 6106%)

2. kecamatan_acc_pusat: Perbandingan ERDKK vs Realisasi ACC PUSAT saja
   • Kriteria ACC PUSAT: mengandung 'disetujui' dan 'pusat', TIDAK mengandung 'menunggu' atau 'ditolak'

3. kios_all: Perbandingan per Kios (semua status)
   • Struktur: KECAMATAN | KODE_KIOS | NAMA_KIOS | [Pupuk] ERDKK | [Pupuk] REALISASI | [Pupuk] SELISIH | [Pupuk] %

4. kios_acc_pusat: Perbandingan per Kios (ACC PUSAT saja)

🎯 PERBAIKAN YANG DITERAPKAN:
1. Perhitungan agregat pupuk yang benar (menjumlahkan semua kolom UREA, NPK, dll)
2. Persentase dihitung sebagai desimal (0.6106) dan diformat sebagai persen (61.06%)
3. Header diformat dengan warna biru dan teks putih
4. Baris TOTAL ditambahkan di akhir dengan background abu-abu
5. Kolom angka diformat dengan 2 desimal dan pemisah ribuan
6. Kolom persentase diformat sebagai persentase

📤 OUTPUT:
Spreadsheet: {OUTPUT_SHEET_URL}
Sheet: kecamatan_all, kecamatan_acc_pusat, kios_all, kios_acc_pusat

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
📁 Repository: verval-pupuk2/scripts/erdkk_vs_realisasi.py
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
    process_erdkk_vs_realisasi()
