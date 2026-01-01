import os
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

# ============================
# KONFIGURASI QUOTA OPTIMIZATION
# ============================
FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"
MAIN_SHEET_URL = "https://docs.google.com/spreadsheets/d/1qcIGC7Vle9O8dOKJNQjyPW7QIymNX-_I44CUthRDQM0/edit"
MONTHLY_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LBxLsPSuba7uDJLYnYRBOWyGUbM30dkBo4SOMJD0Xj0/edit"

# OPTIMIZED RATE LIMITING
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 30
WRITE_DELAY = 5
BATCH_DELAY = 10

EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "sender_email": os.getenv("SENDER_EMAIL"),
    "sender_password": os.getenv("SENDER_EMAIL_PASSWORD"),
    "recipient_emails": [
        email.strip()
        for email in os.getenv("RECIPIENT_EMAILS", "").split(",")
        if email.strip()
    ]
}

# URUTAN BULAN YANG DIINGINKAN
BULAN_URUTAN = [
    "Januari", "Februari", "Maret", "April", "Mei", "Juni",
    "Juli", "Agustus", "September", "Oktober", "November", "Desember"
]

# ============================
# FUNGSI EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """
    Mengirim notifikasi email tentang status proses
    """
    try:
        # Validasi konfigurasi email
        if not EMAIL_CONFIG["sender_email"] or not EMAIL_CONFIG["sender_password"]:
            raise ValueError("EMAIL secrets belum diset (SENDER_EMAIL / SENDER_EMAIL_PASSWORD)")

        if not EMAIL_CONFIG["recipient_emails"]:
            raise ValueError("RECIPIENT_EMAILS kosong")

        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = subject

        # Style untuk email
        if is_success:
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: green;">✅ {subject}</h2>
                    <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                        {message.replace(chr(10), '<br>')}
                    </div>
                    <p><small>Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
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
                    <p><small>Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
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
# FUNGSI BANTU UNTUK FILTER STATUS (DIPERBARUI)
# ============================
def is_status_disetujui_pusat(status_value):
    """
    Cek apakah status termasuk kategori 'Disetujui Pusat'
    Kriteria PERBARUI:
    1. Harus mengandung kata 'disetujui' (case insensitive)
    2. Harus mengandung kata 'pusat' (case insensitive)
    3. TIDAK BOLEH mengandung kata 'menunggu' (case insensitive)
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
    
    # Harus memenuhi semua kriteria
    return contains_disetujui and contains_pusat and not contains_menunggu

def get_all_status_categories(df):
    """Mendapatkan semua kategori status yang ada dalam data"""
    if 'STATUS' not in df.columns:
        return []
    
    status_counts = df['STATUS'].value_counts()
    return status_counts.to_dict()

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
        
        notes = []
        if contains_disetujui and not is_disetujui_pusat:
            if not contains_pusat:
                notes.append("tidak ada 'pusat'")
            if contains_menunggu:
                notes.append("ada 'menunggu'")
        
        note_str = f" ({', '.join(notes)})" if notes else ""
        
        print(f"      {marker} {status}: {count} data ({percentage:.1f}%){note_str}")

# ============================
# FUNGSI BANTU UNTUK PENGURUTAN BULAN
# ============================
def sort_months(month_dict):
    """Mengurutkan bulan berdasarkan urutan yang ditentukan"""
    sorted_months = {}
    
    for bulan in BULAN_URUTAN:
        if bulan in month_dict:
            sorted_months[bulan] = month_dict[bulan]
        # Juga cek variasi penulisan
        for key in list(month_dict.keys()):
            if bulan.lower() in key.lower() and key not in sorted_months:
                sorted_months[bulan] = month_dict[key]
                break
                
    # Tambahkan bulan yang tidak ada di urutan standar
    for key, value in month_dict.items():
        if key not in sorted_months:
            sorted_months[key] = value
            
    return sorted_months

def extract_month_name(filename):
    """Extract bulan dari nama file dan standardisasi"""
    if isinstance(filename, dict):
        filename = filename.get('name', '')
    
    name_without_ext = os.path.splitext(str(filename))[0]
    
    # Cari bulan dalam nama file
    for bulan in BULAN_URUTAN:
        if bulan.lower() in name_without_ext.lower():
            return bulan
    
    # Jika tidak ditemukan, return nama asli (akan diurutkan terakhir)
    return name_without_ext

def create_ordered_monthly_sheets(gc, monthly_pivots, monthly_pivots_acc_pusat):
    """Buat sheet bulanan dengan urutan yang ditentukan"""
    print("\n📊 Membuat sheet bulanan dengan urutan terstruktur...")
    
    # Buka spreadsheet bulanan
    monthly_sheet = safe_google_api_operation(gc.open_by_url, MONTHLY_SHEET_URL)
    
    # Hapus semua sheet kecuali default pertama
    existing_sheets = safe_google_api_operation(monthly_sheet.worksheets)
    if len(existing_sheets) > 1:
        for sheet in existing_sheets[1:]:
            try:
                safe_google_api_operation(monthly_sheet.del_worksheet, sheet)
                print(f"   🗑️  Menghapus sheet: {sheet.title}")
                time.sleep(WRITE_DELAY)
            except Exception as e:
                print(f"   ⚠️  Gagal menghapus {sheet.title}: {str(e)}")
    
    # Standardisasi nama bulan untuk kedua dataset
    standardized_acc_pusat = {}
    standardized_all = {}
    
    # Standardisasi data Disetujui Pusat
    for bulan, data in monthly_pivots_acc_pusat.items():
        std_bulan = extract_month_name(bulan)
        standardized_acc_pusat[std_bulan] = data
    
    # Standardisasi data All
    for bulan, data in monthly_pivots.items():
        std_bulan = extract_month_name(bulan)
        standardized_all[std_bulan] = data
    
    # Urutkan berdasarkan urutan yang ditentukan
    sorted_acc_pusat = sort_months(standardized_acc_pusat)
    sorted_all = sort_months(standardized_all)
    
    print(f"   📅 Data Disetujui Pusat: {list(sorted_acc_pusat.keys())}")
    print(f"   📅 Data All: {list(sorted_all.keys())}")
    
    # Buat sheet untuk Disetujui Pusat terlebih dahulu (akan muncul di kiri)
    sheet_count = 0
    
    print("\n   🟢 MEMBUAT SHEET DISETUJUI PUSAT:")
    for bulan in BULAN_URUTAN:
        if bulan in sorted_acc_pusat:
            sheet_name = f"{bulan}_acc_pusat"
            try:
                worksheet = safe_google_api_operation(
                    monthly_sheet.add_worksheet, 
                    title=sheet_name, 
                    rows="1000", 
                    cols="20"
                )
                
                data = sorted_acc_pusat[bulan]
                safe_google_api_operation(worksheet.clear)
                time.sleep(WRITE_DELAY)
                
                safe_google_api_operation(
                    worksheet.update,
                    [data.columns.values.tolist()] + data.values.tolist()
                )
                
                print(f"      ✅ {sheet_name} ({len(data)} baris)")
                sheet_count += 1
                time.sleep(WRITE_DELAY)
                
            except Exception as e:
                print(f"      ❌ Gagal membuat {sheet_name}: {str(e)}")
    
    # Buat sheet untuk All setelah Disetujui Pusat (akan muncul di kanan)
    print("\n   🔵 MEMBUAT SHEET ALL:")
    for bulan in BULAN_URUTAN:
        if bulan in sorted_all:
            sheet_name = f"{bulan}_all"
            try:
                worksheet = safe_google_api_operation(
                    monthly_sheet.add_worksheet, 
                    title=sheet_name, 
                    rows="1000", 
                    cols="20"
                )
                
                data = sorted_all[bulan]
                safe_google_api_operation(worksheet.clear)
                time.sleep(WRITE_DELAY)
                
                safe_google_api_operation(
                    worksheet.update,
                    [data.columns.values.tolist()] + data.values.tolist()
                )
                
                print(f"      ✅ {sheet_name} ({len(data)} baris)")
                sheet_count += 1
                time.sleep(WRITE_DELAY)
                
            except Exception as e:
                print(f"      ❌ Gagal membuat {sheet_name}: {str(e)}")
    
    # Handle bulan-bulan yang tidak standar (jika ada)
    non_standard_months = set(list(sorted_acc_pusat.keys()) + list(sorted_all.keys())) - set(BULAN_URUTAN)
    
    if non_standard_months:
        print(f"\n   🟡 MEMBUAT SHEET BULAN NON-STANDARD:")
        for bulan in sorted(non_standard_months):
            # Untuk Disetujui Pusat non-standard
            if bulan in sorted_acc_pusat:
                sheet_name = f"{bulan}_acc_pusat"
                try:
                    worksheet = safe_google_api_operation(
                        monthly_sheet.add_worksheet, 
                        title=sheet_name, 
                        rows="1000", 
                        cols="20"
                    )
                    
                    data = sorted_acc_pusat[bulan]
                    safe_google_api_operation(worksheet.clear)
                    time.sleep(WRITE_DELAY)
                    
                    safe_google_api_operation(
                        worksheet.update,
                        [data.columns.values.tolist()] + data.values.tolist()
                    )
                    
                    print(f"      ✅ {sheet_name} ({len(data)} baris)")
                    sheet_count += 1
                    time.sleep(WRITE_DELAY)
                    
                except Exception as e:
                    print(f"      ❌ Gagal membuat {sheet_name}: {str(e)}")
            
            # Untuk All non-standard
            if bulan in sorted_all:
                sheet_name = f"{bulan}_all"
                try:
                    worksheet = safe_google_api_operation(
                        monthly_sheet.add_worksheet, 
                        title=sheet_name, 
                        rows="1000", 
                        cols="20"
                    )
                    
                    data = sorted_all[bulan]
                    safe_google_api_operation(worksheet.clear)
                    time.sleep(WRITE_DELAY)
                    
                    safe_google_api_operation(
                        worksheet.update,
                        [data.columns.values.tolist()] + data.values.tolist()
                    )
                    
                    print(f"      ✅ {sheet_name} ({len(data)} baris)")
                    sheet_count += 1
                    time.sleep(WRITE_DELAY)
                    
                except Exception as e:
                    print(f"      ❌ Gagal membuat {sheet_name}: {str(e)}")
    
    print(f"\n📊 Total sheet bulanan dibuat: {sheet_count}")
    
    return sheet_count

# ============================
# FUNGSI UTAMA YANG DIOPTIMASI
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

def create_pivot_tables(combined_df, monthly_data, pupuk_columns):
    """
    Membuat pivot tables dengan KODE KIOS sebelum NAMA KIOS
    """
    # Pivot per Kecamatan (hanya data agregat, tidak ada KODE KIOS)
    pivot_kecamatan = combined_df.groupby('KECAMATAN')[pupuk_columns].sum().reset_index()
    pivot_kecamatan = pivot_kecamatan.round(2)
    pivot_kecamatan = add_total_row(pivot_kecamatan, pupuk_columns)
    
    # Pivot per Kios (dengan KODE KIOS)
    # Urutan kolom: KECAMATAN, KODE KIOS, NAMA KIOS, lalu pupuk
    pivot_kios = combined_df.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[pupuk_columns].sum().reset_index()
    
    # Urutkan kolom sesuai kebutuhan: KODE KIOS sebelum NAMA KIOS
    pivot_kios = pivot_kios[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + pupuk_columns]
    pivot_kios = pivot_kios.round(2)
    pivot_kios = add_total_row_with_kios(pivot_kios, pupuk_columns)
    
    print(f"   ✅ Pivot Kecamatan: {len(pivot_kecamatan)-1} kecamatan + 1 baris total")
    print(f"   ✅ Pivot Kios: {len(pivot_kios)-1} kios + 1 baris total")

    # Pivot bulanan per Kios
    monthly_pivots = {}
    for bulan, df_bulan in monthly_data.items():
        pivot_bulan = df_bulan.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[pupuk_columns].sum().reset_index()
        
        # Urutkan kolom sesuai kebutuhan
        pivot_bulan = pivot_bulan[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + pupuk_columns]
        pivot_bulan = pivot_bulan.round(2)
        pivot_bulan = add_total_row_with_kios(pivot_bulan, pupuk_columns)
        
        monthly_pivots[bulan] = pivot_bulan
        print(f"   ✅ Pivot {bulan}: {len(pivot_bulan)-1} kios + 1 baris total")

    return pivot_kecamatan, pivot_kios, monthly_pivots

def batch_update_worksheets(spreadsheet, updates):
    print(f"🔄 Memproses batch update untuk {len(updates)} worksheet...")
    
    for i, (sheet_name, data) in enumerate(updates):
        try:
            print(f"   📝 Processing {i+1}/{len(updates)}: {sheet_name}")
            
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"      📝 Menggunakan sheet existing")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = safe_google_api_operation(
                    spreadsheet.add_worksheet, 
                    title=sheet_name, 
                    rows="1000", 
                    cols="20"
                )
                print(f"      ✅ Membuat sheet baru")
                time.sleep(WRITE_DELAY)
            
            safe_google_api_operation(worksheet.clear)
            time.sleep(WRITE_DELAY)
            
            safe_google_api_operation(
                worksheet.update,
                [data.columns.values.tolist()] + data.values.tolist()
            )
            
            print(f"      ✅ Berhasil update data ({len(data)} baris)")
            
            if i < len(updates) - 1:
                time.sleep(WRITE_DELAY)
                
        except Exception as e:
            print(f"      ❌ Gagal update {sheet_name}: {str(e)}")
            continue
    
    print(f"✅ Batch update selesai")

def download_excel_files_from_drive(credentials, folder_id, save_folder="data_excel"):
    """
    Download file Excel dari Google Drive (untuk GitHub Actions)
    """
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    import io

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

def is_dataframe_valid(df):
    """Cek apakah dataframe valid dan tidak kosong"""
    return df is not None and isinstance(df, pd.DataFrame) and not df.empty

def process_verval_pupuk_data_optimized():
    print("🚀 Memulai proses rekap data dengan optimasi quota...")
    print(f"⏰ Konfigurasi:")
    print(f"   - Max retries: {MAX_RETRIES}")
    print(f"   - Write delay: {WRITE_DELAY} detik")
    print(f"   - Urutan bulan: {BULAN_URUTAN}")
    print(f"🔍 Kriteria Disetujui Pusat: mengandung 'disetujui' DAN 'pusat' TANPA 'menunggu'")
    print(f"🏪 Struktur baru: KODE KIOS sebelum NAMA KIOS")

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

    try:
        # Download files dari Google Drive
        excel_files = download_excel_files_from_drive(credentials, FOLDER_ID)
        print(f"📁 Ditemukan {len(excel_files)} file Excel")

        # Process data
        all_data = []
        all_data_acc_pusat = []
        monthly_data = {}
        monthly_data_acc_pusat = {}
        nik_cleaning_log = []
        all_status_categories = set()

        # PERUBAHAN: Tambah KODE KIOS ke expected_columns
        expected_columns = ['KECAMATAN', 'NO TRANSAKSI', 'KODE KIOS', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
                          'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
                          'TGL TEBUS', 'STATUS']

        pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']

        for file_info in excel_files:
            file_path = file_info['path']
            file_name = file_info['name']
            bulan = extract_month_name(file_name)

            print(f"\n📖 Memproses file: {file_name} -> Bulan: {bulan}")

            try:
                df = pd.read_excel(file_path, sheet_name='Worksheet')

                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"   ⚠️  Kolom yang tidak ditemukan: {missing_columns}")
                    continue

                # Track semua status yang ada
                all_status_categories.update(df['STATUS'].astype(str).unique())

                # Clean NIK
                original_nik_count = len(df)
                df['NIK_ORIGINAL'] = df['NIK']
                df['NIK'] = df['NIK'].apply(clean_nik)

                cleaned_niks = df[df['NIK_ORIGINAL'] != df['NIK']][['NIK_ORIGINAL', 'NIK']]
                for _, row in cleaned_niks.iterrows():
                    nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")

                df = df[df['NIK'].notna()]
                cleaned_nik_count = len(df)

                for col in pupuk_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                df['BULAN'] = bulan

                all_data.append(df)

                # Filter data Disetujui Pusat dengan kriteria baru
                df_acc_pusat = df[df['STATUS'].apply(is_status_disetujui_pusat)]
                
                if len(df_acc_pusat) > 0:
                    all_data_acc_pusat.append(df_acc_pusat)
                    if bulan not in monthly_data_acc_pusat:
                        monthly_data_acc_pusat[bulan] = df_acc_pusat
                    else:
                        monthly_data_acc_pusat[bulan] = pd.concat([monthly_data_acc_pusat[bulan], df_acc_pusat])

                if bulan not in monthly_data:
                    monthly_data[bulan] = df
                else:
                    monthly_data[bulan] = pd.concat([monthly_data[bulan], df])

                print(f"   ✅ Berhasil memproses: {cleaned_nik_count} baris data")
                print(f"   ✅ Data Disetujui Pusat: {len(df_acc_pusat)} baris")
                
                # Analisis status untuk file ini
                print_status_analysis(df)

            except Exception as e:
                print(f"   ❌ Error memproses {file_name}: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            print(f"❌ ERROR: {error_msg}")
            send_email_notification("REKAP PIVOT GAGAL", error_msg, is_success=False)
            return

        # Gabungkan data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan (All): {len(combined_df)} baris")

        # Analisis status untuk semua data
        print_status_analysis(combined_df)

        if all_data_acc_pusat:
            combined_df_acc_pusat = pd.concat(all_data_acc_pusat, ignore_index=True)
            print(f"📊 Total data Disetujui Pusat: {len(combined_df_acc_pusat)} baris")
        else:
            combined_df_acc_pusat = None
            print("📊 Tidak ada data dengan status 'Disetujui Pusat'")

        # Buat pivot tables dengan struktur baru
        print("\n📈 Membuat pivot tables untuk semua status...")
        pivot_kecamatan, pivot_kios, monthly_pivots = create_pivot_tables(combined_df, monthly_data, pupuk_columns)

        pivot_kecamatan_acc_pusat = None
        pivot_kios_acc_pusat = None
        monthly_pivots_acc_pusat = {}

        if is_dataframe_valid(combined_df_acc_pusat):
            print("\n📈 Membuat pivot tables untuk Disetujui Pusat...")
            pivot_kecamatan_acc_pusat, pivot_kios_acc_pusat, monthly_pivots_acc_pusat = create_pivot_tables(
                combined_df_acc_pusat, monthly_data_acc_pusat, pupuk_columns
            )

        # Export ke Google Sheets
        print("\n📤 MENGGUNAKAN STRATEGI EXPORT OPTIMIZED...")

        # Buka spreadsheet
        main_sheet = safe_google_api_operation(gc.open_by_url, MAIN_SHEET_URL)

        # Update Main Sheets dengan BATCH
        main_updates = []
        if len(combined_df) > 0:
            main_updates.append(("Kecamatan_all", pivot_kecamatan))
            main_updates.append(("Kios_all", pivot_kios))

        if is_dataframe_valid(combined_df_acc_pusat):
            main_updates.append(("Kecamatan_acc_pusat", pivot_kecamatan_acc_pusat))
            main_updates.append(("Kios_acc_pusat", pivot_kios_acc_pusat))

        if main_updates:
            batch_update_worksheets(main_sheet, main_updates)
            time.sleep(BATCH_DELAY)

        # Buat sheet bulanan dengan urutan yang ditentukan
        monthly_sheet_count = create_ordered_monthly_sheets(gc, monthly_pivots, monthly_pivots_acc_pusat)

        acc_pusat_count = len(combined_df_acc_pusat) if is_dataframe_valid(combined_df_acc_pusat) else 0
        
        # Analisis status yang termasuk Disetujui Pusat
        disetujui_pusat_statuses = [status for status in all_status_categories if is_status_disetujui_pusat(status)]
        
        # Analisis status yang mengandung 'disetujui' tapi tidak termasuk karena alasan tertentu
        ambiguous_statuses = []
        for status in all_status_categories:
            status_lower = str(status).lower()
            contains_disetujui = 'disetujui' in status_lower
            contains_pusat = 'pusat' in status_lower
            contains_menunggu = 'menunggu' in status_lower
            
            if contains_disetujui and not is_status_disetujui_pusat(status):
                reason = []
                if not contains_pusat:
                    reason.append("tidak ada 'pusat'")
                if contains_menunggu:
                    reason.append("mengandung 'menunggu'")
                
                if reason:
                    ambiguous_statuses.append(f"{status} ({', '.join(reason)})")
        
        # Kirim email success
        success_message = f"""
REKAP DATA BERHASIL DENGAN PENAMBAHAN KODE KIOS ✓

📊 STATISTIK:
- File diproses: {len(excel_files)}
- Total data: {len(combined_df):,} baris
- Data Disetujui Pusat: {acc_pusat_count:,} baris
- Sheet dibuat: {len(main_updates)} utama + {monthly_sheet_count} bulanan

🏪 STRUKTUR KOLOM BARU:
- Pivot Kios: KECAMATAN → KODE KIOS → NAMA KIOS → Jenis Pupuk
- Pivot Bulanan: KECAMATAN → KODE KIOS → NAMA KIOS → Jenis Pupuk

🎯 KRITERIA DISETUJUI PUSAT:
1. Harus mengandung kata: 'disetujui'
2. Harus mengandung kata: 'pusat'
3. Tidak boleh mengandung kata: 'menunggu'

✅ STATUS YANG TERMASUK DISETUJUI PUSAT:
{chr(10).join([f"   - {status}" for status in disetujui_pusat_statuses]) if disetujui_pusat_statuses else "   - Tidak ada"}

⚠️ STATUS YANG TIDAK TERMASUK (DENGAN ALASAN):
{chr(10).join([f"   - {status}" for status in ambiguous_statuses]) if ambiguous_statuses else "   - Tidak ada"}

📋 SEMUA STATUS YANG ADA:
{chr(10).join([f"   - {status}" for status in sorted(all_status_categories)])}

📅 URUTAN SHEET BULANAN:
1. Disetujui Pusat: {', '.join(BULAN_URUTAN)}
2. All: {', '.join(BULAN_URUTAN)}

📎 LINK:
- Utama: {MAIN_SHEET_URL}
- Bulanan: {MONTHLY_SHEET_URL}

✅ Sheet bulanan telah diurutkan sesuai permintaan
✅ Kolom KODE KIOS telah ditambahkan sebelum NAMA KIOS
"""
        send_email_notification("REKAP BERHASIL DENGAN PENAMBAHAN KODE KIOS", success_message, is_success=True)

    except Exception as e:
        error_msg = f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        print(f"❌ REKAP GAGAL: {error_msg}")
        send_email_notification("REKAP GAGAL", error_msg, is_success=False)

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_verval_pupuk_data_optimized()
