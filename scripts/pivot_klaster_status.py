"""
pivot_klaster_status.py
Script untuk mengolah data Verval Pupuk dengan pengelompokan berdasarkan status
dan membuat pivot terpisah untuk per Kecamatan dan per Kios.
Dengan tambahan fitur untuk membaca dan menampilkan tanggal input terbaru.

Lokasi: verval-pupuk2/scripts/pivot_klaster_status.py
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
from datetime import datetime, date
import traceback
import json
import time
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"
KECAMATAN_SHEET_URL = "https://docs.google.com/spreadsheets/d/1doC0t-ni1up79sxAIB_uxT9yNlNm5u4FetGbIgLtiK8/edit"
KIOS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1R5ok4B-0AAlZd3gblMViRrlD7hGw4hchynS4tT2d0gc/edit"

# OPTIMIZED RATE LIMITING
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 30
WRITE_DELAY = 5
BATCH_DELAY = 10

# Warna untuk header Google Sheets (RGB values 0-1)
HEADER_FORMAT = {
    "backgroundColor": {"red": 0.0, "green": 0.3, "blue": 0.6},
    "textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
    "horizontalAlignment": "CENTER"
}

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
                    <p><small>📁 Repository: verval-pupuk2/scripts/pivot_klaster_status.py</small></p>
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/pivot_klaster_status.py</small></p>
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
# FUNGSI KLASIFIKASI STATUS
# ============================
def klasifikasikan_status(status_value):
    """
    Versi sederhana: Abaikan kurung, fokus pada status utama
    """
    if pd.isna(status_value) or status_value is None:
        return "TANPA_STATUS"
    
    status_str = str(status_value).lower()
    
    # **STEP 1: ABAIKAN TEKS DALAM KURUNG**
    # Cari dan hapus teks dalam kurung apapun
    bracket_start = status_str.find('(')
    if bracket_start != -1:
        bracket_end = status_str.find(')', bracket_start)
        if bracket_end != -1:
            # Hapus teks dari '(' sampai ')'
            main_status = status_str[:bracket_start] + status_str[bracket_end+1:]
        else:
            # Hanya ada '(' tanpa ')', hapus dari '(' sampai akhir
            main_status = status_str[:bracket_start]
    else:
        main_status = status_str
    
    # Bersihkan spasi ganda
    main_status = ' '.join(main_status.split())
    
    # **STEP 2: KLASIFIKASI BERDASARKAN STATUS UTAMA**
    
    # Kasus 1: MENUNGGU (hanya jika tidak ada "disetujui" di status utama)
    if 'menunggu' in main_status and 'disetujui' not in main_status:
        if 'kecamatan' in main_status:
            return "MENUNGGU_KEC"
        elif 'pusat' in main_status:
            return "MENUNGGU_PUSAT"
    
    # Kasus 2: DISETUJUI
    if 'disetujui' in main_status:
        if 'pusat' in main_status:
            return "DISETUJUI_PUSAT"
        elif 'kecamatan' in main_status:
            return "DISETUJUI_KEC"
    
    # Kasus 3: DITOLAK
    if 'ditolak' in main_status:
        if 'pusat' in main_status:
            return "DITOLAK_PUSAT"
        elif 'kecamatan' in main_status:
            return "DITOLAK_KEC"
    
    # **STEP 3: FALLBACK - cek string asli jika tidak match**
    if 'menunggu' in status_str and 'kecamatan' in status_str:
        return "MENUNGGU_KEC"
    if 'disetujui' in status_str and 'kecamatan' in status_str:
        return "DISETUJUI_KEC"
    if 'disetujui' in status_str and 'pusat' in status_str:
        return "DISETUJUI_PUSAT"
    if 'ditolak' in status_str and 'kecamatan' in status_str:
        return "DITOLAK_KEC"
    if 'ditolak' in status_str and 'pusat' in status_str:
        return "DITOLAK_PUSAT"
    
    # **STEP 4: DEFAULT**
    if 'menunggu' in status_str:
        return "MENUNGGU_LAIN"
    if 'disetujui' in status_str:
        return "DISETUJUI_LAIN"
    if 'ditolak' in status_str:
        return "DITOLAK_LAIN"
    
    return "LAINNYA"

def get_klaster_display_name(klaster):
    """
    Konversi nama klaster untuk tampilan sheet
    """
    mapping = {
        "DISETUJUI_PUSAT": "Setuju_Pusat",
        "DISETUJUI_KEC": "Setuju_Kec",
        "MENUNGGU_KEC": "Menunggu_Kec",
        "MENUNGGU_PUSAT": "Menunggu_Pusat",
        "DITOLAK_PUSAT": "Tolak_Pusat",
        "DITOLAK_KEC": "Tolak_Kec",
        "DITOLAK_LAIN": "Tolak_Lain",
        "MENUNGGU_LAIN": "Menunggu_Lain",
        "DISETUJUI_LAIN": "Setuju_Lain",
        "TANPA_STATUS": "No_Status"
    }
    return mapping.get(klaster, klaster)

# ============================
# FUNGSI BANTU UNTUK TANGGAL INPUT
# ============================
def extract_latest_input_date_from_files(excel_files):
    """
    Mengekstrak tanggal input terbaru dari semua file yang memiliki kolom TGL INPUT
    
    Parameters:
    - excel_files: List informasi file yang didownload
    
    Returns:
    - latest_datetime: datetime.datetime terbaru atau None jika tidak ditemukan
    - found_in_files: Jumlah file yang memiliki kolom TGL INPUT
    """
    latest_datetime = None
    found_in_files = 0
    
    print("📅 Mencari tanggal input dari semua file...")
    
    for file_info in excel_files:
        file_path = file_info['path']
        file_name = file_info['name']
        
        try:
            # Baca file Excel
            df = pd.read_excel(file_path, sheet_name='Worksheet')
            
            # Cek apakah kolom TGL INPUT ada
            tgl_input_cols = [col for col in df.columns if 'TGL INPUT' in col.upper() or 'TANGGAL INPUT' in col.upper()]
            
            if tgl_input_cols:
                # Gunakan kolom pertama yang ditemukan
                tgl_col = tgl_input_cols[0]
                found_in_files += 1
                
                # Konversi ke datetime dan cari nilai yang valid
                # Coba beberapa format tanggal yang mungkin
                try:
                    df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', dayfirst=True)
                except:
                    # Jika gagal, coba format lain
                    try:
                        df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', format='%d/%m/%Y %H:%M:%S')
                    except:
                        try:
                            df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', format='%d-%m-%Y %H:%M:%S')
                        except:
                            try:
                                df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', format='%d/%m/%Y')
                            except:
                                try:
                                    df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', format='%d-%m-%Y')
                                except:
                                    df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce')
                
                valid_datetimes = df[tgl_col].dropna()
                
                if not valid_datetimes.empty:
                    # Cari datetime terbaru dalam file ini
                    file_latest_datetime = valid_datetimes.max()
                    
                    # Update datetime terbaru secara keseluruhan
                    if latest_datetime is None or file_latest_datetime > latest_datetime:
                        latest_datetime = file_latest_datetime
                    
                    # Format untuk display
                    date_str = file_latest_datetime.strftime('%d %b %Y')
                    time_str = file_latest_datetime.strftime('%H:%M:%S') if pd.notna(file_latest_datetime) else "00:00:00"
                    print(f"   ✅ {file_name}: Ditemukan {len(valid_datetimes)} data TGL INPUT, terbaru: {date_str} {time_str}")
                else:
                    print(f"   ⚠️  {file_name}: Kolom '{tgl_col}' ditemukan tapi tidak ada tanggal valid")
            else:
                print(f"   ⚠️  {file_name}: Kolom TGL INPUT tidak ditemukan")
                
        except Exception as e:
            print(f"   ❌ Error membaca tanggal dari {file_name}: {str(e)}")
            continue
    
    if latest_datetime:
        date_str = latest_datetime.strftime('%d %b %Y')
        time_str = latest_datetime.strftime('%H:%M:%S') if pd.notna(latest_datetime) else "00:00:00"
        print(f"📅 Tanggal dan waktu input terbaru ditemukan: {date_str} {time_str}")
    else:
        print("📅 Tidak ditemukan data TGL INPUT yang valid")
    
    return latest_datetime, found_in_files

def format_date_indonesian(date_obj):
    """
    Format tanggal Indonesia dengan bulan singkat (dd mmm yyyy)
    Contoh: 12 Des 2025
    
    Parameters:
    - date_obj: datetime.date object
    
    Returns:
    - String dalam format "dd mmm yyyy"
    """
    if not date_obj:
        return "Tidak tersedia"
    
    # Mapping bulan Indonesia singkat
    bulan_singkat = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 
        5: "Mei", 6: "Jun", 7: "Jul", 8: "Agu",
        9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }
    
    day = date_obj.day
    month = bulan_singkat[date_obj.month]
    year = date_obj.year
    
    return f"{day:02d} {month} {year}"

def format_date_for_sheet(date_obj):
    """
    Format tanggal untuk ditampilkan di sheet (dd mmm yyyy)
    
    Parameters:
    - date_obj: datetime.date object
    
    Returns:
    - String dalam format "dd mmm yyyy" (contoh: "12 Des 2025")
    """
    return format_date_indonesian(date_obj)

def write_update_date_to_sheet(gc, spreadsheet_url, latest_datetime):
    """
    Menulis tanggal dan waktu update ke Sheet1 pada cell E1, E2, dan E3
    
    Parameters:
    - gc: gspread client
    - spreadsheet_url: URL spreadsheet tujuan
    - latest_datetime: Datetime terbaru dalam format datetime.datetime
    """
    try:
        print(f"📝 Menulis tanggal dan waktu update ke Sheet1...")
        
        # Buka spreadsheet
        spreadsheet = safe_google_api_operation(gc.open_by_url, spreadsheet_url)
        
        # Cek apakah Sheet1 ada
        try:
            worksheet = safe_google_api_operation(spreadsheet.worksheet, "Sheet1")
            print("   ✅ Sheet1 ditemukan")
        except gspread.exceptions.WorksheetNotFound:
            # Buat Sheet1 jika tidak ada
            print("   📄 Sheet1 tidak ditemukan, membuat baru...")
            worksheet = safe_google_api_operation(
                spreadsheet.add_worksheet,
                title="Sheet1",
                rows="100",
                cols="20"
            )
        
        # Bersihkan cell E1, E2, dan E3 sebelum menulis
        safe_google_api_operation(worksheet.update, 'E1', [['']])
        safe_google_api_operation(worksheet.update, 'E2', [['']])
        safe_google_api_operation(worksheet.update, 'E3', [['']])
        time.sleep(WRITE_DELAY)
        
        # Tulis data ke cell E1, E2, dan E3
        safe_google_api_operation(worksheet.update, 'E1', [['Update per tanggal input']])
        time.sleep(WRITE_DELAY)
        
        # Format tanggal untuk E2
        if latest_datetime:
            date_formatted = format_date_for_sheet(latest_datetime.date())
        else:
            date_formatted = "Tanggal tidak tersedia"
        
        safe_google_api_operation(worksheet.update, 'E2', [[date_formatted]])
        time.sleep(WRITE_DELAY)
        
        # Format jam untuk E3
        if latest_datetime:
            time_formatted = latest_datetime.strftime('%H:%M:%S') if pd.notna(latest_datetime) else "00:00:00"
        else:
            time_formatted = "Waktu tidak tersedia"
        
        safe_google_api_operation(worksheet.update, 'E3', [[time_formatted]])
        
        # Format header (cell E1)
        worksheet.format('E1:E1', {
            "textFormat": {"bold": True, "fontSize": 11},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE"
        })
        
        # Format tanggal (cell E2)
        worksheet.format('E2:E2', {
            "textFormat": {"bold": True, "fontSize": 12},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
        })
        
        # Format jam (cell E3)
        worksheet.format('E3:E3', {
            "textFormat": {"bold": True, "fontSize": 12},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.92}
        })
        
        # Atur tinggi baris untuk E1, E2, dan E3
        worksheet.format('E1:E3', {
            "wrapStrategy": "WRAP"
        })
        
        print(f"   ✅ Tanggal dan waktu update berhasil ditulis:")
        print(f"      E2: {date_formatted}")
        print(f"      E3: {time_formatted}")
        return True
        
    except Exception as e:
        print(f"   ❌ Gagal menulis tanggal dan waktu update: {str(e)}")
        return False

# ============================
# FUNGSI BANTU LAINNYA
# ============================
def clean_nik(nik_value):
    if pd.isna(nik_value) or nik_value is None:
        return None
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)
    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")
    return cleaned_nik if cleaned_nik else None

def exponential_backoff(attempt):
    base_delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 1))
    jitter = base_delay * 0.1
    return base_delay + jitter

def safe_google_api_operation(operation, *args, **kwargs):
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

def add_total_row(df, pupuk_columns):
    """
    Menambahkan baris TOTAL untuk pivot kecamatan (tanpa KODE KIOS)
    """
    df_with_total = df.copy()
    
    total_row = {col: df[col].sum() for col in pupuk_columns}
    first_col = df.columns[0]  # Biasanya 'KECAMATAN'
    total_row[first_col] = "TOTAL"
    
    # Isi kolom lainnya dengan string kosong
    for col in df.columns:
        if col not in pupuk_columns and col != first_col:
            total_row[col] = ""
    
    total_df = pd.DataFrame([total_row])
    df_with_total = pd.concat([df_with_total, total_df], ignore_index=True)
    
    return df_with_total

def add_total_row_with_kios(df, pupuk_columns):
    """
    Menambahkan baris TOTAL untuk pivot dengan KODE KIOS
    """
    df_with_total = df.copy()
    
    # Buat baris total
    total_row = {col: df[col].sum() for col in pupuk_columns}
    
    # Set kolom non-numerik
    total_row['KECAMATAN'] = "TOTAL"
    total_row['KODE KIOS'] = ""  # Kosong untuk KODE KIOS
    total_row['NAMA KIOS'] = ""  # Kosong untuk NAMA KIOS
    
    # Isi kolom lainnya dengan string kosong
    for col in df.columns:
        if col not in pupuk_columns and col not in ['KECAMATAN', 'KODE KIOS', 'NAMA KIOS']:
            total_row[col] = ""
    
    # Tambahkan baris total
    total_df = pd.DataFrame([total_row])
    df_with_total = pd.concat([df_with_total, total_df], ignore_index=True)
    
    return df_with_total

def apply_header_format(gc, spreadsheet_url, sheet_name):
    """
    Menerapkan format header pada sheet
    """
    try:
        spreadsheet = safe_google_api_operation(gc.open_by_url, spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Format baris pertama (header)
        worksheet.format('A1:Z1', HEADER_FORMAT)
        
        # Auto-resize kolom
        worksheet.columns_auto_resize(0, 20)
        
        print(f"   🎨 Format header diterapkan pada {sheet_name}")
        return True
    except Exception as e:
        print(f"   ⚠️  Gagal format header {sheet_name}: {str(e)}")
        return False

# ============================
# FUNGSI DOWNLOAD FILE
# ============================
def download_excel_files_from_drive(credentials, folder_id, save_folder="data_excel"):
    """
    Download file Excel dari Google Drive (untuk GitHub Actions)
    """
    os.makedirs(save_folder, exist_ok=True)
    drive_service = build('drive', 'v3', credentials=credentials)

    # Query untuk mencari file Excel
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ Tidak ada file Excel di folder Google Drive.")

    paths = []
    for file in files:
        print(f"📥 Downloading: {file['name']}")
        request = drive_service.files().get_media(fileId=file["id"])
        
        # Gunakan nama file yang aman
        safe_filename = "".join(c for c in file['name'] if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
        file_path = os.path.join(save_folder, safe_filename)

        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

        paths.append({
            'path': file_path,
            'name': file['name'],
            'id': file['id']
        })

    print(f"✅ Berhasil download {len(paths)} file Excel")
    return paths

# ============================
# FUNGSI PEMROSESAN DATA UTAMA
# ============================
def create_pivot_klaster(df, numeric_columns, pivot_type='kecamatan'):
    """
    Membuat pivot berdasarkan klaster status
    
    Parameters:
    - df: DataFrame
    - numeric_columns: kolom numerik yang akan dijumlahkan
    - pivot_type: 'kecamatan' atau 'kios'
    
    Returns:
    - Dictionary dengan klaster sebagai key dan pivot DataFrame sebagai value
    """
    pivots = {}
    
    # Tambah kolom KLASIFIKASI_STATUS
    df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(klasifikasikan_status)
    
    # Kelompokkan berdasarkan klaster
    for klaster in df['KLASIFIKASI_STATUS'].unique():
        df_klaster = df[df['KLASIFIKASI_STATUS'] == klaster].copy()
        
        if pivot_type == 'kecamatan':
            # Group by KECAMATAN
            pivot = df_klaster.groupby('KECAMATAN')[numeric_columns].sum().reset_index()
            pivot = add_total_row(pivot, numeric_columns)
            
        elif pivot_type == 'kios':
            # Group by KECAMATAN, KODE KIOS, NAMA KIOS
            pivot = df_klaster.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[numeric_columns].sum().reset_index()
            
            # Urutkan kolom sesuai kebutuhan: KODE KIOS sebelum NAMA KIOS
            pivot = pivot[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + numeric_columns]
            pivot = add_total_row_with_kios(pivot, numeric_columns)
        
        # Format numerik
        for col in numeric_columns:
            if col in pivot.columns:
                pivot[col] = pivot[col].round(2)
        
        pivots[klaster] = pivot
    
    return pivots

def process_and_upload_pivots(gc, df, numeric_columns, spreadsheet_url, pivot_type, latest_datetime=None):
    """
    Memproses dan mengupload pivot ke Google Sheets
    
    Parameters:
    - gc: gspread client
    - df: DataFrame utama
    - numeric_columns: kolom numerik
    - spreadsheet_url: URL spreadsheet tujuan
    - pivot_type: 'kecamatan' atau 'kios'
    - latest_datetime: Datetime input terbaru untuk ditulis di Sheet1
    """
    print(f"\n📊 Membuat pivot {pivot_type} berdasarkan klaster status...")
    
    # Buat pivot berdasarkan klaster
    pivots = create_pivot_klaster(df, numeric_columns, pivot_type)
    
    # Buka spreadsheet
    spreadsheet = safe_google_api_operation(gc.open_by_url, spreadsheet_url)
    
    # Tulis tanggal update ke Sheet1 jika ada tanggal
    if latest_datetime:
        write_update_date_to_sheet(gc, spreadsheet_url, latest_datetime)
    
    # Hapus semua sheet kecuali Sheet1
    existing_sheets = safe_google_api_operation(spreadsheet.worksheets)
    sheets_to_delete = [sheet for sheet in existing_sheets if sheet.title != "Sheet1"]
    
    if sheets_to_delete:
        for sheet in sheets_to_delete:
            try:
                safe_google_api_operation(spreadsheet.del_worksheet, sheet)
                print(f"   🗑️  Menghapus sheet: {sheet.title}")
                time.sleep(WRITE_DELAY)
            except Exception as e:
                print(f"   ⚠️  Gagal menghapus {sheet.title}: {str(e)}")
    
    # Upload setiap klaster
    sheet_count = 0
    for klaster, pivot_df in pivots.items():
        sheet_name = get_klaster_display_name(klaster)
        row_count = len(pivot_df)
        
        print(f"   📝 {sheet_name}: {row_count-1} baris data + 1 total")
        
        try:
            # Buat worksheet baru
            worksheet = safe_google_api_operation(
                spreadsheet.add_worksheet, 
                title=sheet_name, 
                rows=str(row_count + 10), 
                cols=str(len(pivot_df.columns) + 5)
            )
            
            # Upload data
            safe_google_api_operation(worksheet.clear)
            time.sleep(WRITE_DELAY)
            
            safe_google_api_operation(
                worksheet.update,
                [pivot_df.columns.values.tolist()] + pivot_df.values.tolist()
            )
            
            # Terapkan format header
            time.sleep(WRITE_DELAY)
            apply_header_format(gc, spreadsheet_url, sheet_name)
            
            sheet_count += 1
            time.sleep(WRITE_DELAY)
            
        except Exception as e:
            print(f"   ❌ Gagal membuat sheet {sheet_name}: {str(e)}")
    
    print(f"📊 Total {pivot_type} sheet dibuat: {sheet_count}")
    return sheet_count

def analyze_status_distribution(df):
    """
    Analisis distribusi status
    """
    print("\n📈 ANALISIS DISTRIBUSI STATUS:")
    
    # Tambah kolom klasifikasi
    df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(klasifikasikan_status)
    
    # Hitung distribusi
    status_counts = df['KLASIFIKASI_STATUS'].value_counts()
    total_data = len(df)
    
    for status, count in status_counts.items():
        percentage = (count / total_data) * 100
        display_name = get_klaster_display_name(status)
        print(f"   📌 {display_name}: {count:,} data ({percentage:.1f}%)")
    
    return status_counts.to_dict()

# ============================
# FUNGSI UTAMA
# ============================
def process_verval_pupuk_by_klaster():
    print("=" * 60)
    print("🚀 PROSES REKAP DATA BERDASARKAN KLASTER STATUS")
    print("=" * 60)
    print(f"📁 Repository: verval-pupuk2/scripts/pivot_klaster_status.py")
    print(f"⏰ Konfigurasi:")
    print(f"   - Max retries: {MAX_RETRIES}")
    print(f"   - Write delay: {WRITE_DELAY} detik")
    print(f"   - Pivot Kecamatan: {KECAMATAN_SHEET_URL}")
    print(f"   - Pivot Kios: {KIOS_SHEET_URL}")
    print("=" * 60)

    try:
        # ========== LOAD CREDENTIALS ==========
        creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan di environment variables")

        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )

        gc = gspread.authorize(credentials)

        # Download files dari Google Drive
        excel_files = download_excel_files_from_drive(credentials, FOLDER_ID)
        print(f"📁 Ditemukan {len(excel_files)} file Excel")

        # EKSTRAK TANGGAL INPUT TERBARU
        latest_datetime, files_with_date = extract_latest_input_date_from_files(excel_files)
        
        # Konfigurasi kolom
        expected_columns = ['KECAMATAN', 'NO TRANSAKSI', 'KODE KIOS', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
                          'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
                          'TGL TEBUS', 'STATUS']
        
        pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']

        all_data = []

        # Proses setiap file
        for file_info in excel_files:
            file_path = file_info['path']
            file_name = file_info['name']

            print(f"\n📖 Memproses: {file_name}")

            try:
                df = pd.read_excel(file_path, sheet_name='Worksheet')

                # Cek kolom yang ada
                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"   ⚠️  Kolom yang tidak ditemukan: {missing_columns}")
                    continue

                # Clean NIK
                df['NIK'] = df['NIK'].apply(clean_nik)
                df = df[df['NIK'].notna()]

                # Konversi kolom pupuk ke numerik
                for col in pupuk_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                all_data.append(df)
                print(f"   ✅ Berhasil: {len(df)} baris")

            except Exception as e:
                print(f"   ❌ Error memproses {file_name}: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            print(f"❌ ERROR: {error_msg}")
            send_email_notification("REKAP KLASTER GAGAL", error_msg, is_success=False)
            return

        # Gabungkan semua data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan: {len(combined_df):,} baris")

        # Analisis distribusi status
        status_dist = analyze_status_distribution(combined_df)

        # Proses dan upload pivot kecamatan (DENGAN TANGGAL UPDATE)
        kecamatan_sheet_count = process_and_upload_pivots(
            gc, combined_df, pupuk_columns, KECAMATAN_SHEET_URL, 'kecamatan', latest_datetime
        )

        # Proses dan upload pivot kios (DENGAN TANGGAL UPDATE)
        kios_sheet_count = process_and_upload_pivots(
            gc, combined_df, pupuk_columns, KIOS_SHEET_URL, 'kios', latest_datetime
        )

        # Siapkan summary untuk email
        status_summary = "\n".join([
            f"   • {get_klaster_display_name(k)}: {v:,} data" 
            for k, v in status_dist.items()
        ])
        
        # Tambah info tanggal update
        date_info = ""
        if latest_datetime:
            date_formatted_display = format_date_indonesian(latest_datetime.date())
            time_formatted_display = latest_datetime.strftime('%H:%M:%S') if pd.notna(latest_datetime) else "00:00:00"
            date_info = f"\n📅 INFORMASI TANGGAL & WAKTU:\n   • Tanggal input terbaru: {date_formatted_display}\n   • Jam input terbaru: {time_formatted_display}\n   • Format di sheet E2: '{date_formatted_display}'\n   • Format di sheet E3: '{time_formatted_display}'\n   • File dengan TGL INPUT: {files_with_date}/{len(excel_files)}"
        else:
            date_info = f"\n📅 INFORMASI TANGGAL & WAKTU:\n   • Tanggal input: Tidak ditemukan di file sumber\n   • Format di sheet E2: 'Tanggal tidak tersedia'\n   • Format di sheet E3: 'Waktu tidak tersedia'\n   • File dengan TGL INPUT: 0/{len(excel_files)}"

        success_message = f"""
REKAP DATA BERDASARKAN KLASTER STATUS BERHASIL ✓

📊 STATISTIK UMUM:
• Repository: verval-pupuk2/scripts/pivot_klaster_status.py
• File diproses: {len(excel_files)}
• Total data: {len(combined_df):,} baris
• Sheet Kecamatan: {kecamatan_sheet_count} klaster
• Sheet Kios: {kios_sheet_count} klaster

{date_info}

📋 DISTRIBUSI STATUS:
{status_summary}

🏢 STRUKTUR PIVOT:
1. PER KECAMATAN:
   • Kolom: KECAMATAN → Jenis Pupuk
   • Setiap klaster jadi sheet terpisah
   • Baris TOTAL di akhir setiap sheet
   • Header berwarna biru dengan teks putih

2. PER KIOS:
   • Kolom: KECAMATAN → KODE KIOS → NAMA KIOS → Jenis Pupuk
   • Setiap klaster jadi sheet terpisah
   • Baris TOTAL di akhir setiap sheet
   • Header berwarna biru dengan teks putih

📍 INFO TANGGAL UPDATE:
• Tanggal update ditampilkan di Sheet1 cell E1-E3
• Cell E1: "Update per tanggal input"
• Cell E2: Tanggal dalam format "dd mmm yyyy" (contoh: "12 Des 2025")
• Cell E3: Jam dalam format "HH:MM:SS" (contoh: "14:30:45")
• Format bold dengan background berbeda untuk E2 dan E3

🔗 LINK HASIL:
• Pivot Kecamatan: {KECAMATAN_SHEET_URL}
• Pivot Kios: {KIOS_SHEET_URL}

🎯 FITUR:
✅ Pengelompokan otomatis berdasarkan status
✅ Nama sheet singkat dan deskriptif
✅ Baris TOTAL di setiap sheet
✅ Format header profesional
✅ File terpisah untuk kecamatan dan kios
✅ Tampilan tanggal input terbaru di Sheet1
✅ Format tanggal Indonesia (dd mmm yyyy)
"""

        send_email_notification("REKAP KLASTER BERHASIL", success_message, is_success=True)
        print("\n" + "=" * 60)
        print("✅ PROSES SELESAI DENGAN SUKSES!")
        print("=" * 60)

    except Exception as e:
        error_msg = f"""
Repository: verval-pupuk2/scripts/pivot_klaster_status.py
Error: {str(e)}

Traceback:
{traceback.format_exc()}
"""
        print(f"\n❌ PROSES GAGAL")
        print(f"❌ {str(e)}")
        send_email_notification("REKAP KLASTER GAGAL", error_msg, is_success=False)

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_verval_pupuk_by_klaster()
