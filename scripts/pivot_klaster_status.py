"""
pivot_klaster_status.py - VERSI DIPERBAIKI DENGAN DEBUGGING MENDALAM
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
FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"
KECAMATAN_SHEET_URL = "https://docs.google.com/spreadsheets/d/11-fOg3AdSodQeOUwYqkK7GlWSTc1r_1t7pmKGbF9cWI/edit"
KIOS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1lCPLDLKOtiiUfMCM9cnYv_vXSxaPbbR-TOeLKuBvWxc/edit"

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
# FUNGSI KLASIFIKASI STATUS - VERSI SUPER KETAT
# ============================
def klasifikasikan_status(status_value):
    """
    Klasifikasi status dengan logika SUPER KETAT:
    1. Hapus SEMUA konten dalam kurung () [] {} <>
    2. Hanya klasifikasi berdasarkan teks di LUAR kurung
    3. LOGIKA: Jika ada "menunggu" di teks utama -> MENUNGGU, jika ada "disetujui" -> DISETUJUI
    """
    if pd.isna(status_value) or status_value is None:
        return "TANPA_STATUS"
    
    status_str = str(status_value).lower().strip()
    
    # DEBUG: Simpan status asli
    original_status = status_str
    
    # **HAPUS SEMUA KONTEN DALAM KURUNG APAPUN**
    import re
    
    # Hapus semua kurung dan isinya: (), [], {}, <>
    status_no_brackets = re.sub(r'[\(\[{<].*?[\)\]}>]', '', status_str)
    
    # Bersihkan spasi berlebihan
    status_no_brackets = re.sub(r'\s+', ' ', status_no_brackets).strip()
    
    # Jika setelah hapus kurung jadi kosong, gunakan string asli
    if not status_no_brackets:
        status_no_brackets = status_str
    
    # **LOGIKA KLASIFIKASI YANG SANGAT KETAT**
    
    # PENTING: Urutan pengecekan MATTER!
    
    # 1. Cek apakah status utama (tanpa kurung) mengandung "menunggu"
    #    Ini untuk menangkap "Menunggu verifikasi tim verval kecamatan"
    if 'menunggu' in status_no_brackets:
        # Cek lebih spesifik
        if 'kecamatan' in status_no_brackets and 'disetujui' not in status_no_brackets:
            return "MENUNGGU_KEC"
        elif 'pusat' in status_no_brackets and 'disetujui' not in status_no_brackets:
            return "MENUNGGU_PUSAT"
        else:
            return "MENUNGGU_LAIN"
    
    # 2. Cek apakah status utama (tanpa kurung) mengandung "disetujui"
    #    Ini untuk menangkap "Disetujui tim verval kecamatan"
    elif 'disetujui' in status_no_brackets:
        if 'pusat' in status_no_brackets:
            return "DISETUJUI_PUSAT"
        elif 'kecamatan' in status_no_brackets:
            return "DISETUJUI_KEC"
        else:
            return "DISETUJUI_LAIN"
    
    # 3. Cek apakah status utama (tanpa kurung) mengandung "ditolak"
    elif 'ditolak' in status_no_brackets:
        if 'pusat' in status_no_brackets:
            return "DITOLAK_PUSAT"
        elif 'kecamatan' in status_no_brackets:
            return "DITOLAK_KEC"
        else:
            return "DITOLAK_LAIN"
    
    # 4. FALLBACK: Coba cari di string lengkap (dengan kurung)
    #    Tapi dengan prioritas: DISETUJUI > DITOLAK > MENUNGGU
    if 'disetujui' in status_str:
        if 'pusat' in status_str:
            return "DISETUJUI_PUSAT"
        elif 'kecamatan' in status_str:
            return "DISETUJUI_KEC"
        else:
            return "DISETUJUI_LAIN"
    
    if 'ditolak' in status_str:
        if 'pusat' in status_str:
            return "DITOLAK_PUSAT"
        elif 'kecamatan' in status_str:
            return "DITOLAK_KEC"
        else:
            return "DITOLAK_LAIN"
    
    if 'menunggu' in status_str:
        if 'kecamatan' in status_str:
            return "MENUNGGU_KEC"
        elif 'pusat' in status_str:
            return "MENUNGGU_PUSAT"
        else:
            return "MENUNGGU_LAIN"
    
    # 5. Default
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
        "TANPA_STATUS": "No_Status",
        "LAINNYA": "Lainnya"
    }
    return mapping.get(klaster, klaster)

# ============================
# FUNGSI DEBUG STATUS
# ============================
def debug_status_classification(df, sample_size=10):
    """
    Debug fungsi klasifikasi status dengan menampilkan contoh-contoh
    """
    print("\n🔍 DEBUG KLASIFIKASI STATUS:")
    print("=" * 80)
    
    if 'STATUS' not in df.columns:
        print("❌ Kolom STATUS tidak ditemukan!")
        return
    
    unique_statuses = df['STATUS'].dropna().unique()
    print(f"📊 Total status unik: {len(unique_statuses)}")
    
    # Analisis untuk setiap status unik
    status_summary = {}
    for status in unique_statuses[:sample_size]:
        classification = klasifikasikan_status(status)
        
        # Cari apakah ada kurung
        has_brackets = '(' in str(status) or ')' in str(status)
        
        print(f"\n📝 Status: '{status[:80]}...'")
        print(f"   🏷️  Klasifikasi: {classification}")
        print(f"   📎 Ada kurung: {'✅' if has_brackets else '❌'}")
        
        # Simpan untuk summary
        if classification not in status_summary:
            status_summary[classification] = []
        status_summary[classification].append(status)
    
    # Tampilkan summary
    print(f"\n📈 SUMMARY KLASIFIKASI:")
    for classification, statuses in status_summary.items():
        print(f"   • {classification}: {len(statuses)} jenis status")
        for status in statuses[:3]:  # Tampilkan 3 contoh pertama
            print(f"     - '{status[:60]}...'")
    
    return status_summary

# ============================
# FUNGSI BANTU UNTUK TANGGAL INPUT
# ============================
def extract_latest_input_date_from_files(excel_files):
    latest_datetime = None
    found_in_files = 0
    
    print("📅 Mencari tanggal input dari semua file...")
    
    for file_info in excel_files:
        file_path = file_info['path']
        file_name = file_info['name']
        
        try:
            df = pd.read_excel(file_path, sheet_name='Worksheet')
            
            tgl_input_cols = [col for col in df.columns if 'TGL INPUT' in col.upper() or 'TANGGAL INPUT' in col.upper()]
            
            if tgl_input_cols:
                tgl_col = tgl_input_cols[0]
                found_in_files += 1
                
                try:
                    df[tgl_col] = pd.to_datetime(df[tgl_col], errors='coerce', dayfirst=True)
                except:
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
                    file_latest_datetime = valid_datetimes.max()
                    
                    if latest_datetime is None or file_latest_datetime > latest_datetime:
                        latest_datetime = file_latest_datetime
                    
                    date_str = file_latest_datetime.strftime('%d %b %Y')
                    time_str = file_latest_datetime.strftime('%H:%M:%S') if pd.notna(file_latest_datetime) else "00:00:00"
                    print(f"   ✅ {file_name}: Terbaru: {date_str} {time_str}")
                else:
                    print(f"   ⚠️  {file_name}: Tidak ada tanggal valid")
            else:
                print(f"   ⚠️  {file_name}: Kolom TGL INPUT tidak ditemukan")
                
        except Exception as e:
            print(f"   ❌ Error membaca tanggal dari {file_name}: {str(e)}")
            continue
    
    if latest_datetime:
        date_str = latest_datetime.strftime('%d %b %Y')
        time_str = latest_datetime.strftime('%H:%M:%S') if pd.notna(latest_datetime) else "00:00:00"
        print(f"📅 Tanggal dan waktu input terbaru: {date_str} {time_str}")
    else:
        print("📅 Tidak ditemukan data TGL INPUT yang valid")
    
    return latest_datetime, found_in_files

def format_date_indonesian(date_obj):
    if not date_obj:
        return "Tidak tersedia"
    
    bulan_singkat = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 
        5: "Mei", 6: "Jun", 7: "Jul", 8: "Agu",
        9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
    }
    
    day = date_obj.day
    month = bulan_singkat[date_obj.month]
    year = date_obj.year
    
    return f"{day:02d} {month} {year}"

def write_update_date_to_sheet(gc, spreadsheet_url, latest_datetime):
    try:
        print(f"📝 Menulis tanggal dan waktu update ke Sheet1...")
        
        spreadsheet = safe_google_api_operation(gc.open_by_url, spreadsheet_url)
        
        try:
            worksheet = spreadsheet.worksheet("Sheet1")
        except:
            worksheet = spreadsheet.add_worksheet(title="Sheet1", rows="100", cols="20")
        
        worksheet.update('E1', [['Update per tanggal input']])
        time.sleep(WRITE_DELAY)
        
        if latest_datetime:
            date_formatted = format_date_indonesian(latest_datetime.date())
        else:
            date_formatted = "Tanggal tidak tersedia"
        
        worksheet.update('E2', [[date_formatted]])
        time.sleep(WRITE_DELAY)
        
        if latest_datetime:
            time_formatted = latest_datetime.strftime('%H:%M:%S')
        else:
            time_formatted = "Waktu tidak tersedia"
        
        worksheet.update('E3', [[time_formatted]])
        
        print(f"   ✅ Tanggal update: {date_formatted} {time_formatted}")
        return True
        
    except Exception as e:
        print(f"   ❌ Gagal menulis tanggal: {str(e)}")
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
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik}")
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
                    print(f"⏳ Quota exceeded, menunggu {wait_time:.1f} detik...")
                    time.sleep(wait_time)
                else:
                    raise e
            elif e.resp.status in [500, 502, 503, 504]:
                if attempt < MAX_RETRIES:
                    wait_time = exponential_backoff(attempt)
                    print(f"⏳ Server error {e.resp.status}, menunggu {wait_time:.1f} detik...")
                    time.sleep(wait_time)
                else:
                    raise e
            else:
                raise e
        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                wait_time = exponential_backoff(attempt)
                print(f"⏳ Error {type(e).__name__}, menunggu {wait_time:.1f} detik...")
                time.sleep(wait_time)
            else:
                raise e
    
    raise last_exception

def add_total_row(df, pupuk_columns):
    df_with_total = df.copy()
    
    total_row = {col: df[col].sum() for col in pupuk_columns}
    first_col = df.columns[0]
    total_row[first_col] = "TOTAL"
    
    for col in df.columns:
        if col not in pupuk_columns and col != first_col:
            total_row[col] = ""
    
    total_df = pd.DataFrame([total_row])
    df_with_total = pd.concat([df_with_total, total_df], ignore_index=True)
    
    return df_with_total

def add_total_row_with_kios(df, pupuk_columns):
    df_with_total = df.copy()
    
    total_row = {col: df[col].sum() for col in pupuk_columns}
    
    total_row['KECAMATAN'] = "TOTAL"
    total_row['KODE KIOS'] = ""
    total_row['NAMA KIOS'] = ""
    
    for col in df.columns:
        if col not in pupuk_columns and col not in ['KECAMATAN', 'KODE KIOS', 'NAMA KIOS']:
            total_row[col] = ""
    
    total_df = pd.DataFrame([total_row])
    df_with_total = pd.concat([df_with_total, total_df], ignore_index=True)
    
    return df_with_total

def apply_header_format(gc, spreadsheet_url, sheet_name):
    try:
        spreadsheet = gc.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        worksheet.format('A1:Z1', HEADER_FORMAT)
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
    os.makedirs(save_folder, exist_ok=True)
    drive_service = build('drive', 'v3', credentials=credentials)

    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ Tidak ada file Excel di folder Google Drive.")

    paths = []
    for file in files:
        print(f"📥 Downloading: {file['name']}")
        request = drive_service.files().get_media(fileId=file["id"])
        
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
    pivots = {}
    
    # PASTIKAN kolom KLASIFIKASI_STATUS sudah ada
    if 'KLASIFIKASI_STATUS' not in df.columns:
        print("   ⚠️  Membuat kolom KLASIFIKASI_STATUS...")
        df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(klasifikasikan_status)
    
    # DEBUG: Hitung distribusi per klaster
    print("\n   📊 DISTRIBUSI PER KLASTER:")
    status_counts = df['KLASIFIKASI_STATUS'].value_counts()
    for klaster, count in status_counts.items():
        print(f"      • {klaster}: {count:,} data")
    
    # Kelompokkan berdasarkan klaster
    for klaster in df['KLASIFIKASI_STATUS'].unique():
        df_klaster = df[df['KLASIFIKASI_STATUS'] == klaster].copy()
        
        print(f"   📁 Processing klaster '{klaster}': {len(df_klaster)} baris")
        
        # DEBUG: Tampilkan contoh status untuk klaster ini
        sample_statuses = df_klaster['STATUS'].dropna().unique()[:2]
        for i, status in enumerate(sample_statuses):
            print(f"      Contoh {i+1}: '{status[:70]}...'")
        
        if pivot_type == 'kecamatan':
            pivot = df_klaster.groupby('KECAMATAN')[numeric_columns].sum().reset_index()
            pivot = add_total_row(pivot, numeric_columns)
            
        elif pivot_type == 'kios':
            pivot = df_klaster.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[numeric_columns].sum().reset_index()
            pivot = pivot[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + numeric_columns]
            pivot = add_total_row_with_kios(pivot, numeric_columns)
        
        for col in numeric_columns:
            if col in pivot.columns:
                pivot[col] = pivot[col].round(2)
        
        pivots[klaster] = pivot
    
    return pivots

def process_and_upload_pivots(gc, df, numeric_columns, spreadsheet_url, pivot_type, latest_datetime=None):
    print(f"\n📊 Membuat pivot {pivot_type} berdasarkan klaster status...")
    
    pivots = create_pivot_klaster(df, numeric_columns, pivot_type)
    
    spreadsheet = safe_google_api_operation(gc.open_by_url, spreadsheet_url)
    
    if latest_datetime:
        write_update_date_to_sheet(gc, spreadsheet_url, latest_datetime)
    
    # HAPUS SEMUA SHEET LAMA (kecuali Sheet1)
    existing_sheets = spreadsheet.worksheets()
    for sheet in existing_sheets:
        if sheet.title != "Sheet1":
            try:
                spreadsheet.del_worksheet(sheet)
                print(f"   🗑️  Menghapus sheet lama: {sheet.title}")
                time.sleep(WRITE_DELAY)
            except:
                pass
    
    sheet_count = 0
    for klaster, pivot_df in pivots.items():
        sheet_name = get_klaster_display_name(klaster)
        row_count = len(pivot_df)
        
        print(f"   📝 Uploading {sheet_name}: {row_count-1} baris data")
        
        try:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, 
                rows=str(row_count + 10), 
                cols=str(len(pivot_df.columns) + 5)
            )
            
            worksheet.clear()
            time.sleep(WRITE_DELAY)
            
            worksheet.update(
                [pivot_df.columns.values.tolist()] + pivot_df.values.tolist()
            )
            
            time.sleep(WRITE_DELAY)
            apply_header_format(gc, spreadsheet_url, sheet_name)
            
            sheet_count += 1
            time.sleep(WRITE_DELAY)
            
        except Exception as e:
            print(f"   ❌ Gagal membuat sheet {sheet_name}: {str(e)}")
    
    print(f"📊 Total {pivot_type} sheet dibuat: {sheet_count}")
    return sheet_count

# ============================
# FUNGSI UTAMA YANG DIPERBAIKI
# ============================
def process_verval_pupuk_by_klaster():
    print("=" * 80)
    print("🚀 PROSES REKAP DATA BERDASARKAN KLASTER STATUS - DEBUG VERSION")
    print("=" * 80)

    try:
        # Load credentials
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

        # Download files
        excel_files = download_excel_files_from_drive(credentials, FOLDER_ID)
        print(f"📁 Ditemukan {len(excel_files)} file Excel")

        latest_datetime, files_with_date = extract_latest_input_date_from_files(excel_files)
        
        expected_columns = ['KECAMATAN', 'NO TRANSAKSI', 'KODE KIOS', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
                          'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
                          'TGL TEBUS', 'STATUS']
        
        pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']

        all_data = []

        for file_info in excel_files:
            file_path = file_info['path']
            file_name = file_info['name']

            print(f"\n📖 Memproses: {file_name}")

            try:
                df = pd.read_excel(file_path, sheet_name='Worksheet')

                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"   ⚠️  Missing: {missing_columns}")
                    continue

                # Clean data
                df['NIK'] = df['NIK'].apply(clean_nik)
                df = df[df['NIK'].notna()]

                for col in pupuk_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # **DEBUG: Analisis status dalam file ini**
                if 'STATUS' in df.columns:
                    print(f"   🔍 Analisis status dalam file:")
                    status_counts = df['STATUS'].value_counts()
                    for status, count in status_counts.head(5).items():
                        classification = klasifikasikan_status(status)
                        print(f"      • '{status[:50]}...' → {classification}: {count} data")
                
                all_data.append(df)
                print(f"   ✅ Berhasil: {len(df)} baris")

            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            send_email_notification("REKAP KLASTER GAGAL", error_msg, is_success=False)
            return

        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan: {len(combined_df):,} baris")
        
        # **DEBUG EXTENSIF: Analisis status sebelum klasifikasi**
        print("\n" + "=" * 80)
        print("🔍 DEBUG ANALISIS STATUS DETAIL")
        print("=" * 80)
        
        # 1. Hitung total baris dengan status
        total_with_status = combined_df['STATUS'].notna().sum()
        print(f"📈 Total data dengan status: {total_with_status:,} ({total_with_status/len(combined_df)*100:.1f}%)")
        
        # 2. Analisis pola status
        unique_statuses = combined_df['STATUS'].dropna().unique()
        print(f"📝 Jumlah status unik: {len(unique_statuses)}")
        
        # 3. Klasifikasi semua data
        print("\n🎯 MENERAPKAN KLASIFIKASI STATUS...")
        combined_df['KLASIFIKASI_STATUS'] = combined_df['STATUS'].apply(klasifikasikan_status)
        
        # 4. Analisis setelah klasifikasi
        print("\n📊 DISTRIBUSI SETELAH KLASIFIKASI:")
        status_counts = combined_df['KLASIFIKASI_STATUS'].value_counts()
        total_classified = status_counts.sum()
        
        for status, count in status_counts.items():
            percentage = (count / total_classified) * 100
            print(f"   • {status}: {count:,} data ({percentage:.1f}%)")
        
        # 5. DEBUG khusus untuk MENUNGGU_KEC
        if "MENUNGGU_KEC" in status_counts:
            print(f"\n⚠️  DEBUG DATA MENUNGGU_KEC:")
            menunggu_kec_data = combined_df[combined_df['KLASIFIKASI_STATUS'] == "MENUNGGU_KEC"]
            print(f"   Total data MENUNGGU_KEC: {len(menunggu_kec_data):,}")
            
            # Tampilkan contoh status yang diklasifikasikan sebagai MENUNGGU_KEC
            sample_statuses = menunggu_kec_data['STATUS'].dropna().unique()[:10]
            print(f"   Contoh status yang jadi MENUNGGU_KEC:")
            for i, status in enumerate(sample_statuses):
                print(f"     {i+1}. '{status}'")
        
        # Clear old sheets
        print(f"\n🗑️  MEMBERSIHKAN SHEET LAMA...")
        
        for url in [KECAMATAN_SHEET_URL, KIOS_SHEET_URL]:
            try:
                spreadsheet = gc.open_by_url(url)
                sheets = spreadsheet.worksheets()
                for sheet in sheets:
                    if sheet.title != "Sheet1":
                        spreadsheet.del_worksheet(sheet)
                        print(f"   ✅ Menghapus: {sheet.title}")
                        time.sleep(WRITE_DELAY)
            except Exception as e:
                print(f"   ⚠️  Gagal clear {url}: {str(e)}")
        
        # Process pivots
        kecamatan_sheet_count = process_and_upload_pivots(
            gc, combined_df, pupuk_columns, KECAMATAN_SHEET_URL, 'kecamatan', latest_datetime
        )

        kios_sheet_count = process_and_upload_pivots(
            gc, combined_df, pupuk_columns, KIOS_SHEET_URL, 'kios', latest_datetime
        )

        # Prepare success message
        success_message = f"""
REKAP DATA BERDASARKAN KLASTER STATUS BERHASIL ✓

📊 STATISTIK UMUM:
• File diproses: {len(excel_files)}
• Total data: {len(combined_df):,} baris
• Data dengan status: {total_with_status:,} ({total_with_status/len(combined_df)*100:.1f}%)
• Status unik: {len(unique_statuses)}
• Sheet Kecamatan: {kecamatan_sheet_count} klaster
• Sheet Kios: {kios_sheet_count} klaster

📋 DISTRIBUSI STATUS:
"""
        for status, count in status_counts.items():
            percentage = (count / total_classified) * 100
            display_name = get_klaster_display_name(status)
            success_message += f"• {display_name}: {count:,} data ({percentage:.1f}%)\n"

        success_message += f"""
🎯 LOGIKA KLASIFIKASI:
• Mengabaikan SEMUA konten dalam kurung () [] {{}} <>
• Hanya klasifikasi berdasarkan teks DI LUAR kurung
• Contoh: "Disetujui tim verval kecamatan (menunggu...)" → Setuju_Kec
• Contoh: "Menunggu verifikasi tim verval kecamatan" → Menunggu_Kec

🔗 LINK HASIL:
• Pivot Kecamatan: {KECAMATAN_SHEET_URL}
• Pivot Kios: {KIOS_SHEET_URL}
"""

        send_email_notification("REKAP KLASTER BERHASIL - DEBUG", success_message, is_success=True)
        print("\n" + "=" * 80)
        print("✅ PROSES SELESAI DENGAN SUKSES!")
        print("=" * 80)

    except Exception as e:
        error_msg = f"""
Repository: verval-pupuk2/scripts/pivot_klaster_status.py
Error: {str(e)}

Traceback:
{traceback.format_exc()}
"""
        print(f"\n❌ PROSES GAGAL: {str(e)}")
        send_email_notification("REKAP KLASTER GAGAL", error_msg, is_success=False)

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_verval_pupuk_by_klaster()
