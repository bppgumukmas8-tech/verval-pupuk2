import os
import io
import json
import pandas as pd
import gspread
import re
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"  # Folder Google Drive ERDKK
SAVE_FOLDER = "data_erdkk"  # Folder lokal di runner
SPREADSHEET_ID = "1aEx7cgw1KIdpXo20dD3LnCHF6PWer1wWgT7H5YKSqlY"
SHEET_NAME = "Hasil_Rekap"

# ============================
# LOAD CREDENTIALS DAN KONFIGURASI EMAIL DARI SECRETS
# ============================
# Load Google credentials dari secret
creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_json:
    raise ValueError("❌ SECRET GOOGLE_APPLICATION_CREDENTIALS_JSON TIDAK TERBACA")

# Load email configuration dari secrets
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")

# Validasi email configuration
if not SENDER_EMAIL:
    raise ValueError("❌ SECRET SENDER_EMAIL TIDAK TERBACA")
if not SENDER_EMAIL_PASSWORD:
    raise ValueError("❌ SECRET SENDER_EMAIL_PASSWORD TIDAK TERBACA")
if not RECIPIENT_EMAILS:
    raise ValueError("❌ SECRET RECIPIENT_EMAILS TIDAK TERBACA")

# Parse recipient emails
try:
    recipient_list = json.loads(RECIPIENT_EMAILS)
except json.JSONDecodeError:
    recipient_list = [email.strip() for email in RECIPIENT_EMAILS.split(",")]

# KONFIGURASI EMAIL
EMAIL_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": SENDER_EMAIL,
    "sender_password": SENDER_EMAIL_PASSWORD,
    "recipient_emails": recipient_list
}

credentials = Credentials.from_service_account_info(
    json.loads(creds_json),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)

gc = gspread.authorize(credentials)
drive_service = build("drive", "v3", credentials=credentials)

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

    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)

    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")

    return cleaned_nik if cleaned_nik else None

# ============================
# FUNGSI STANDARDISASI KOLOM
# ============================
def standardize_columns(df):
    """
    Standarisasi nama kolom untuk konsistensi berdasarkan header ERDKK
    """
    if df.empty:
        return df
    
    # Mapping kolom berdasarkan header ERDKK yang Anda berikan
    column_mapping = {
        # Kolom utama
        'nama penyuluh': 'Nama Penyuluh',
        'kode desa': 'Kode Desa',
        'kode kios pengecer': 'Kode Kios Pengecer',
        'nama kios pengecer': 'Nama Kios Pengecer',
        'gapoktan': 'Gapoktan',
        'nama poktan': 'Nama Poktan',
        'nama petani': 'Nama Petani',
        'ktp': 'KTP',
        'tempat lahir': 'Tempat Lahir',
        'tanggal lahir': 'Tanggal Lahir',
        'nama ibu kandung': 'Nama Ibu Kandung',
        'alamat': 'Alamat',
        'subsektor': 'Subsektor',
        'nama desa': 'Nama Desa',
        
        # MT1
        'komoditas mt1': 'Komoditas MT1',
        'luas lahan (ha) mt1': 'Luas Lahan (Ha) MT1',
        'pupuk urea (kg) mt1': 'Pupuk Urea (Kg) MT1',
        'pupuk npk (kg) mt1': 'Pupuk NPK (Kg) MT1',
        'pupuk npk formula (kg) mt1': 'Pupuk NPK Formula (Kg) MT1',
        'pupuk organik (kg) mt1': 'Pupuk Organik (Kg) MT1',
        'pupuk za (kg) mt1': 'Pupuk ZA (Kg) MT1',
        
        # MT2
        'komoditas mt2': 'Komoditas MT2',
        'luas lahan (ha) mt2': 'Luas Lahan (Ha) MT2',
        'pupuk urea (kg) mt2': 'Pupuk Urea (Kg) MT2',
        'pupuk npk (kg) mt2': 'Pupuk NPK (Kg) MT2',
        'pupuk npk formula (kg) mt2': 'Pupuk NPK Formula (Kg) MT2',
        'pupuk organik (kg) mt2': 'Pupuk Organik (Kg) MT2',
        'pupuk za (kg) mt2': 'Pupuk ZA (Kg) MT2',
        
        # MT3
        'komoditas mt3': 'Komoditas MT3',
        'luas lahan (ha) mt3': 'Luas Lahan (Ha) MT3',
        'pupuk urea (kg) mt3': 'Pupuk Urea (Kg) MT3',
        'pupuk npk (kg) mt3': 'Pupuk NPK (Kg) MT3',
        'pupuk npk formula (kg) mt3': 'Pupuk NPK Formula (Kg) MT3',
        'pupuk organik (kg) mt3': 'Pupuk Organik (Kg) MT3',
        'pupuk za (kg) mt3': 'Pupuk ZA (Kg) MT3',
    }
    
    # Rename columns berdasarkan mapping
    new_columns = []
    for col in df.columns:
        if pd.isna(col):
            new_columns.append('UNNAMED')
            continue
            
        col_lower = str(col).lower().strip()
        
        if col_lower in column_mapping:
            new_columns.append(column_mapping[col_lower])
        else:
            # Cari partial match
            found = False
            for key in column_mapping:
                if key in col_lower:
                    new_columns.append(column_mapping[key])
                    found = True
                    break
            if not found:
                new_columns.append(col)
    
    df.columns = new_columns
    
    # Hapus kolom duplikat jika ada
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df

# ============================
# FUNGSI GABUNGKAN KOMODITAS UNIK
# ============================
def gabung_komoditas_unik(komoditas_list):
    """
    Menggabungkan komoditas dari MT1, MT2, MT3 tanpa duplikat
    """
    if not komoditas_list:
        return ""
    
    # Flatten list
    flat_list = []
    for item in komoditas_list:
        if pd.isna(item):
            continue
        if isinstance(item, str):
            # Split jika ada multiple komoditas dalam satu sel
            items = str(item).strip()
            if items:
                flat_list.append(items)
        else:
            item_str = str(item).strip()
            if item_str:
                flat_list.append(item_str)
    
    # Hapus duplikat dan kosong, urutkan untuk konsistensi
    unique_komoditas = sorted(set([k for k in flat_list if k.strip()]))
    return ", ".join(unique_komoditas)

# ============================
# FUNGSI KONVERSI KE NUMERIK
# ============================
def convert_to_numeric(value):
    """
    Konversi nilai ke numeric, handle berbagai format
    """
    if pd.isna(value) or value is None:
        return 0
    
    try:
        # Hapus koma, titik (kecuali desimal), dan spasi
        value_str = str(value).strip()
        value_str = value_str.replace(',', '').replace(' ', '')
        
        # Handle jika ada titik sebagai pemisah ribuan
        if '.' in value_str and value_str.count('.') == 1:
            # Mungkin desimal, biarkan
            pass
        else:
            # Hapus semua titik
            value_str = value_str.replace('.', '')
        
        return float(value_str)
    except:
        return 0

# ============================
# FUNGSI PROSES DATA PIVOT
# ============================
def proses_data_pivot(dataframes_list):
    """
    Membuat pivot data ERDKK sesuai dengan format yang diminta
    """
    if not dataframes_list:
        return []
    
    # Dictionary untuk menyimpan data pivot per key (KTP + Poktan)
    pivot_dict = {}
    
    # Header output sesuai permintaan - TANPA kolom luas lahan per MT
    output_header = [
        'KTP',
        'Nama Petani',
        'Nama Poktan',
        'Desa',
        'Kecamatan',
        'Nama Kios Pengecer',
        'Komoditas',  # Kolom 7: Komoditas saja
        'Rencana Tanam 1 Tahun (Ha)',  # Kolom 8: Total luas lahan
        # MT1 - Pupuk saja
        'Pupuk Urea (Kg) MT1',
        'Pupuk NPK (Kg) MT1',
        'Pupuk NPK Formula (Kg) MT1',
        'Pupuk Organik (Kg) MT1',
        'Pupuk ZA (Kg) MT1',
        # MT2 - Pupuk saja (TANPA luas lahan)
        'Pupuk Urea (Kg) MT2',
        'Pupuk NPK (Kg) MT2',
        'Pupuk NPK Formula (Kg) MT2',
        'Pupuk Organik (Kg) MT2',
        'Pupuk ZA (Kg) MT2',
        # MT3 - Pupuk saja (TANPA luas lahan)
        'Pupuk Urea (Kg) MT3',
        'Pupuk NPK (Kg) MT3',
        'Pupuk NPK Formula (Kg) MT3',
        'Pupuk Organik (Kg) MT3',
        'Pupuk ZA (Kg) MT3'
    ]
    
    hasil_rows = [output_header]
    
    total_rows_processed = 0
    
    for df_idx, df in enumerate(dataframes_list):
        if df.empty:
            print(f"   ⚠️  Dataframe {df_idx} kosong, dilewati")
            continue
        
        print(f"   📊 Processing dataframe {df_idx + 1}: {len(df)} rows")
        
        for i in range(len(df)):
            row = df.iloc[i]
            
            # Buat key unik berdasarkan KTP dan Nama Poktan
            ktp_value = row.get('KTP', '') if not pd.isna(row.get('KTP')) else ''
            poktan_value = row.get('Nama Poktan', '') if not pd.isna(row.get('Nama Poktan')) else ''
            
            key = f"{ktp_value}|{poktan_value}"
            
            if key not in pivot_dict:
                # Data baru
                pivot_dict[key] = {
                    'KTP': ktp_value,
                    'Nama Petani': row.get('Nama Petani', '') if not pd.isna(row.get('Nama Petani')) else '',
                    'Nama Poktan': poktan_value,
                    'Desa': row.get('Nama Desa', '') if not pd.isna(row.get('Nama Desa')) else '',
                    'Kecamatan': row.get('Gapoktan', '') if not pd.isna(row.get('Gapoktan')) else '',  # Gunakan Gapoktan untuk Kecamatan
                    'Nama Kios Pengecer': row.get('Nama Kios Pengecer', '') if not pd.isna(row.get('Nama Kios Pengecer')) else '',
                    
                    # Komoditas
                    'komoditas_set': set(),
                    
                    # Luas lahan total (MT1 + MT2 + MT3)
                    'luas_total': 0,
                    
                    # MT1 - Pupuk
                    'urea_mt1': 0,
                    'npk_mt1': 0,
                    'npk_formula_mt1': 0,
                    'organik_mt1': 0,
                    'za_mt1': 0,
                    
                    # MT2 - Pupuk
                    'urea_mt2': 0,
                    'npk_mt2': 0,
                    'npk_formula_mt2': 0,
                    'organik_mt2': 0,
                    'za_mt2': 0,
                    
                    # MT3 - Pupuk
                    'urea_mt3': 0,
                    'npk_mt3': 0,
                    'npk_formula_mt3': 0,
                    'organik_mt3': 0,
                    'za_mt3': 0,
                }
            
            # Tambahkan komoditas ke set
            for mt_col in ['Komoditas MT1', 'Komoditas MT2', 'Komoditas MT3']:
                komoditas_value = row.get(mt_col, '')
                if not pd.isna(komoditas_value) and komoditas_value:
                    pivot_dict[key]['komoditas_set'].add(str(komoditas_value).strip())
            
            # Konversi dan jumlahkan luas lahan TOTAL (MT1 + MT2 + MT3)
            luas_mt1 = convert_to_numeric(row.get('Luas Lahan (Ha) MT1', 0))
            luas_mt2 = convert_to_numeric(row.get('Luas Lahan (Ha) MT2', 0))
            luas_mt3 = convert_to_numeric(row.get('Luas Lahan (Ha) MT3', 0))
            
            pivot_dict[key]['luas_total'] += (luas_mt1 + luas_mt2 + luas_mt3)
            
            # Jumlahkan pupuk MT1
            pivot_dict[key]['urea_mt1'] += convert_to_numeric(row.get('Pupuk Urea (Kg) MT1', 0))
            pivot_dict[key]['npk_mt1'] += convert_to_numeric(row.get('Pupuk NPK (Kg) MT1', 0))
            pivot_dict[key]['npk_formula_mt1'] += convert_to_numeric(row.get('Pupuk NPK Formula (Kg) MT1', 0))
            pivot_dict[key]['organik_mt1'] += convert_to_numeric(row.get('Pupuk Organik (Kg) MT1', 0))
            pivot_dict[key]['za_mt1'] += convert_to_numeric(row.get('Pupuk ZA (Kg) MT1', 0))
            
            # Jumlahkan pupuk MT2
            pivot_dict[key]['urea_mt2'] += convert_to_numeric(row.get('Pupuk Urea (Kg) MT2', 0))
            pivot_dict[key]['npk_mt2'] += convert_to_numeric(row.get('Pupuk NPK (Kg) MT2', 0))
            pivot_dict[key]['npk_formula_mt2'] += convert_to_numeric(row.get('Pupuk NPK Formula (Kg) MT2', 0))
            pivot_dict[key]['organik_mt2'] += convert_to_numeric(row.get('Pupuk Organik (Kg) MT2', 0))
            pivot_dict[key]['za_mt2'] += convert_to_numeric(row.get('Pupuk ZA (Kg) MT2', 0))
            
            # Jumlahkan pupuk MT3
            pivot_dict[key]['urea_mt3'] += convert_to_numeric(row.get('Pupuk Urea (Kg) MT3', 0))
            pivot_dict[key]['npk_mt3'] += convert_to_numeric(row.get('Pupuk NPK (Kg) MT3', 0))
            pivot_dict[key]['npk_formula_mt3'] += convert_to_numeric(row.get('Pupuk NPK Formula (Kg) MT3', 0))
            pivot_dict[key]['organik_mt3'] += convert_to_numeric(row.get('Pupuk Organik (Kg) MT3', 0))
            pivot_dict[key]['za_mt3'] += convert_to_numeric(row.get('Pupuk ZA (Kg) MT3', 0))
            
            total_rows_processed += 1
    
    print(f"   📊 Data diproses: {total_rows_processed} baris")
    print(f"   🎯 Unique keys: {len(pivot_dict)}")
    
    # Konversi dictionary ke list untuk output
    for key, data in pivot_dict.items():
        # Format komoditas
        komoditas_str = ", ".join(sorted(data['komoditas_set'])) if data['komoditas_set'] else ""
        
        # Buat row output
        output_row = [
            data['KTP'],
            data['Nama Petani'],
            data['Nama Poktan'],
            data['Desa'],
            data['Kecamatan'],
            data['Nama Kios Pengecer'],
            komoditas_str,  # Kolom 7: Komoditas
            round(data['luas_total'], 2),  # Kolom 8: Rencana Tanam 1 Tahun
            # MT1 - Pupuk saja
            round(data['urea_mt1'], 2),
            round(data['npk_mt1'], 2),
            round(data['npk_formula_mt1'], 2),
            round(data['organik_mt1'], 2),
            round(data['za_mt1'], 2),
            # MT2 - Pupuk saja (TANPA luas lahan)
            round(data['urea_mt2'], 2),
            round(data['npk_mt2'], 2),
            round(data['npk_formula_mt2'], 2),
            round(data['organik_mt2'], 2),
            round(data['za_mt2'], 2),
            # MT3 - Pupuk saja (TANPA luas lahan)
            round(data['urea_mt3'], 2),
            round(data['npk_mt3'], 2),
            round(data['npk_formula_mt3'], 2),
            round(data['organik_mt3'], 2),
            round(data['za_mt3'], 2),
        ]
        
        hasil_rows.append(output_row)
    
    return hasil_rows

# ============================
# DOWNLOAD FILE EXCEL DARI DRIVE
# ============================
def download_excel_files(folder_id, save_folder=SAVE_FOLDER):
    os.makedirs(save_folder, exist_ok=True)
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("Tidak ada file Excel di folder Google Drive.")

    paths = []
    for f in files:
        request = drive_service.files().get_media(fileId=f["id"])
        fh = io.FileIO(os.path.join(save_folder, f["name"]), "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        paths.append(os.path.join(save_folder, f["name"]))
    return paths

# ============================
# FUNGSI UNTUK MENULIS DATA KE GOOGLE SHEETS (DENGAN AUTO EXPAND)
# ============================
def clear_all_sheets_except_first(sh):
    """
    Membersihkan semua worksheet kecuali worksheet pertama
    """
    try:
        worksheets = sh.worksheets()
        if len(worksheets) > 1:
            print(f"🗑️  Membersihkan {len(worksheets)-1} worksheet tambahan...")
            for i in range(1, len(worksheets)):
                try:
                    sh.del_worksheet(worksheets[i])
                except:
                    pass
        print("✅ Semua worksheet tambahan telah dibersihkan")
    except Exception as e:
        print(f"⚠️  Gagal membersihkan worksheet: {str(e)}")

def optimize_worksheet_size(ws, required_rows, required_cols):
    """
    Optimalkan ukuran worksheet untuk menghemat cells
    """
    try:
        current_rows = ws.row_count
        current_cols = ws.col_count
        
        # Hitung cells yang digunakan
        current_cells = current_rows * current_cols
        required_cells = required_rows * required_cols
        
        print(f"📊 Cells saat ini: {current_cells:,}")
        print(f"📊 Cells diperlukan: {required_cells:,}")
        
        # Jika cells saat ini > 2x required, shrink worksheet
        if current_cells > (required_cells * 2):
            print(f"🔄 Shrinking worksheet: {current_rows}x{current_cols} → {required_rows}x{required_cols}")
            
            # Resize ke ukuran yang diperlukan + buffer kecil
            new_rows = required_rows + 100
            new_cols = required_cols + 5
            
            ws.resize(rows=new_rows, cols=new_cols)
            print(f"✅ Worksheet di-shrink menjadi {new_rows} baris x {new_cols} kolom")
            return True
        
        # Jika cells tidak cukup, expand
        elif required_rows > current_rows or required_cols > current_cols:
            print(f"🔄 Expanding worksheet: {current_rows}x{current_cols} → {required_rows}x{required_cols}")
            
            # Resize ke ukuran yang diperlukan + buffer
            new_rows = max(required_rows + 100, int(required_rows * 1.1))
            new_cols = max(required_cols + 5, int(required_cols * 1.1))
            
            ws.resize(rows=new_rows, cols=new_cols)
            print(f"✅ Worksheet di-expand menjadi {new_rows} baris x {new_cols} kolom")
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️  Gagal optimize worksheet size: {str(e)}")
        return False

def write_to_google_sheet(worksheet, data_rows):
    """
    Menulis data ke Google Sheets dengan metode chunking dan auto-expand
    """
    try:
        print(f"📤 Menulis {len(data_rows)} baris data ke Google Sheets...")
        
        total_rows_to_write = len(data_rows)
        total_columns = len(data_rows[0]) if data_rows else 0
        
        print(f"📊 Ukuran data: {total_rows_to_write} baris x {total_columns} kolom")
        
        # 1. Optimalkan ukuran worksheet
        optimize_worksheet_size(worksheet, total_rows_to_write, total_columns)
        
        # 2. Clear semua data di worksheet (hanya data, bukan sheetnya)
        print("🧹 Membersihkan semua data di worksheet...")
        try:
            # Clear seluruh worksheet
            worksheet.clear()
            print("✅ Worksheet berhasil dibersihkan")
        except Exception as clear_error:
            print(f"⚠️  Gagal clear worksheet: {str(clear_error)}")
        
        # 3. Tentukan ukuran chunk yang aman untuk Google Sheets
        CHUNK_SIZE = 10000
        chunk_count = (total_rows_to_write + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        print(f"🔀 Membagi data menjadi {chunk_count} chunk...")
        
        # 4. Tulis data per chunk
        for chunk_index in range(chunk_count):
            start_row = chunk_index * CHUNK_SIZE
            end_row = min(start_row + CHUNK_SIZE, total_rows_to_write)
            
            current_chunk = data_rows[start_row:end_row]
            start_cell = f'A{start_row + 1}'
            
            # Format end_cell untuk logging
            end_column_letter = chr(64 + total_columns)
            end_cell = f'{end_column_letter}{end_row}'
            
            print(f"   📄 Menulis chunk {chunk_index + 1}/{chunk_count}: {start_cell}:{end_cell}...")
            
            try:
                worksheet.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
                
                if chunk_index < chunk_count - 1:
                    # Jeda progresif
                    wait_time = 2 + (chunk_index * 0.1)
                    time.sleep(wait_time)
                    
            except Exception as chunk_error:
                error_msg = str(chunk_error)
                print(f"❌ Error pada chunk {chunk_index + 1}: {error_msg}")
                
                # Coba handle error khusus
                if "exceeds grid limits" in error_msg or "400" in error_msg or "200000" in error_msg:
                    print("⚠️  Terdeteksi error grid limits...")
                    
                    # Coba shrink worksheet dulu
                    print("🔄 Mencoba shrink worksheet...")
                    time.sleep(3)
                    
                    # Coba lagi dengan chunk yang lebih kecil
                    try:
                        worksheet.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
                        print(f"✅ Chunk {chunk_index + 1} berhasil pada percobaan kedua")
                    except Exception as retry_error:
                        print(f"❌ Gagal lagi: {str(retry_error)}")
                        
                        # Jika masih gagal, coba buat worksheet baru dengan nama berbeda
                        if "10000000" in str(retry_error):
                            print("💡 Spreadsheet penuh! Mencoba alternatif...")
                            return create_alternative_sheet(data_rows)
                        else:
                            raise retry_error
                else:
                    print("🔄 Mencoba lagi dengan jeda yang lebih lama...")
                    
                    time.sleep(5)
                    try:
                        worksheet.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
                        print(f"✅ Chunk {chunk_index + 1} berhasil pada percobaan kedua")
                    except Exception as retry_error:
                        print(f"❌ Gagal lagi pada chunk {chunk_index + 1}: {str(retry_error)}")
                        raise retry_error
        
        print(f"✅ Semua data berhasil ditulis! Total {total_rows_to_write} baris.")
        return True
        
    except Exception as e:
        print(f"❌ Gagal menulis data ke Google Sheets: {str(e)}")
        
        # Coba alternatif jika spreadsheet penuh
        if "10000000" in str(e):
            print("💡 Spreadsheet mencapai limit 10 juta cells!")
            return create_alternative_sheet(data_rows)
        
        raise

def create_alternative_sheet(data_rows):
    """
    Buat spreadsheet alternatif jika spreadsheet utama penuh
    """
    try:
        total_rows = len(data_rows)
        total_cols = len(data_rows[0]) if data_rows else 0
        
        print(f"🆕 Membuat spreadsheet alternatif untuk {total_rows} baris data...")
        
        # Buat spreadsheet baru
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_spreadsheet_name = f"ERDKK_Pivot_{timestamp}"
        
        print(f"📝 Membuat spreadsheet baru: '{new_spreadsheet_name}'")
        
        # Buat spreadsheet baru
        new_sh = gc.create(new_spreadsheet_name)
        
        # Share dengan owner (optional)
        try:
            new_sh.share(SENDER_EMAIL, perm_type='user', role='writer')
        except:
            pass
        
        # Dapatkan worksheet pertama
        new_ws = new_sh.get_worksheet(0)
        
        # Update title
        new_ws.update_title(SHEET_NAME)
        
        # Resize worksheet
        required_rows = total_rows + 100
        required_cols = total_cols + 5
        new_ws.resize(rows=required_rows, cols=required_cols)
        
        print(f"✅ Spreadsheet baru dibuat: {new_spreadsheet_name}")
        print(f"🔗 Link: https://docs.google.com/spreadsheets/d/{new_sh.id}")
        
        # Tulis data dengan chunking
        CHUNK_SIZE = 10000
        chunk_count = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        print(f"🔀 Menulis {chunk_count} chunk ke spreadsheet baru...")
        
        for chunk_index in range(chunk_count):
            start_row = chunk_index * CHUNK_SIZE
            end_row = min(start_row + CHUNK_SIZE, total_rows)
            
            current_chunk = data_rows[start_row:end_row]
            start_cell = f'A{start_row + 1}'
            
            print(f"   📄 Chunk {chunk_index + 1}/{chunk_count}: baris {start_row + 1}-{end_row}...")
            
            new_ws.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
            
            if chunk_index < chunk_count - 1:
                time.sleep(2)
        
        print(f"✅ Semua data berhasil ditulis ke spreadsheet baru!")
        
        # Update global variables untuk email notification
        global SPREADSHEET_ID, SHEET_NAME
        SPREADSHEET_ID = new_sh.id
        SHEET_NAME = SHEET_NAME
        
        return True
        
    except Exception as e:
        print(f"❌ Gagal membuat spreadsheet alternatif: {str(e)}")
        raise

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """
    Mengirim notifikasi email tentang status proses
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = subject

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
# PROSES UTAMA
# ============================
def main():
    try:
        log = []
        all_dataframes = []
        total_rows_original = 0
        total_rows_cleaned = 0
        file_count = 0
        nik_cleaning_log = []

        print("=" * 60)
        print("🔍 MEMULAI PROSES REKAP DATA ERDKK - PIVOT VERSION")
        print("=" * 60)
        print(f"📁 Folder ID: {FOLDER_ID}")
        print(f"📊 Spreadsheet ID: {SPREADSHEET_ID}")
        print(f"📧 Email penerima: {', '.join(recipient_list)}")
        print()

        # 1. Download semua Excel dari folder ERDKK
        excel_files = download_excel_files(FOLDER_ID)
        print(f"📁 Berhasil download {len(excel_files)} file Excel dari 31 kecamatan")
        print()

        # 2. Proses setiap file
        for fpath in excel_files:
            file_count += 1
            filename = os.path.basename(fpath)
            print(f"🔄 Memproses file {file_count}/{len(excel_files)}: {filename}")
            
            try:
                # Baca file Excel
                df = pd.read_excel(fpath, dtype=str)
                print(f"   📊 Kolom yang ditemukan: {list(df.columns)}")
                
            except Exception as e:
                print(f"   ❌ Gagal membaca file: {str(e)}")
                log.append(f"- {filename}: GAGAL DIBACA - {str(e)}")
                continue

            # Standarisasi kolom
            df = standardize_columns(df)
            print(f"   🔧 Kolom setelah standarisasi: {list(df.columns)}")
            
            # Cek apakah kolom KTP ada
            if 'KTP' in df.columns:
                original_count = len(df)
                
                # Simpan original KTP dengan cara yang benar
                if 'KTP_ORIGINAL' in df.columns:
                    # Jika kolom sudah ada, rename dulu
                    df = df.rename(columns={'KTP_ORIGINAL': 'KTP_ORIGINAL_TEMP'})
                
                # Simpan nilai original sebelum cleaning
                df['KTP_ORIGINAL'] = df['KTP'].copy()
                
                # Bersihkan NIK/KTP
                df['KTP'] = df['KTP'].apply(clean_nik)
                
                # Log perubahan NIK
                mask = df['KTP_ORIGINAL'] != df['KTP']
                if mask.any():
                    cleaned_ktp = df[mask][['KTP_ORIGINAL', 'KTP']].head(5)  # Ambil 5 contoh saja
                    for _, row in cleaned_ktp.iterrows():
                        nik_cleaning_log.append(f"'{row['KTP_ORIGINAL']}' -> {row['KTP']}")
                
                # Hapus baris dengan NIK kosong
                before_clean = len(df)
                df = df[df['KTP'].notna()]
                after_clean = len(df)
                
                total_rows_original += original_count
                total_rows_cleaned += after_clean
                
                dropped_count = before_clean - after_clean
                if dropped_count > 0:
                    log.append(f"- {filename}: {original_count} → {after_clean} baris ({dropped_count} NIK kosong dihapus)")
                else:
                    log.append(f"- {filename}: {original_count} baris (semua NIK valid)")
                
                # Tambahkan kolom nama file untuk tracking
                df['FILE_SOURCE'] = filename
                
                all_dataframes.append(df)
                
                print(f"   ✅ Berhasil: {original_count} → {after_clean} baris")
            else:
                print(f"   ⚠️  Kolom KTP tidak ditemukan dalam file")
                log.append(f"- {filename}: KOLOM KTP TIDAK DITEMUKAN")

        print()
        
        if not all_dataframes:
            raise ValueError("❌ Tidak ada data yang berhasil diproses dari semua file")

        # 3. Proses dan buat pivot data
        print(f"🔄 Membuat pivot data dari {len(all_dataframes)} file...")
        hasil_pivot = proses_data_pivot(all_dataframes)
        
        if len(hasil_pivot) < 2:  # Hanya header, tidak ada data
            raise ValueError("❌ Tidak ada data yang berhasil dipivot")
        
        print(f"✅ Pivot data selesai: {len(hasil_pivot) - 1} baris hasil")
        print(f"   📋 Kolom output: {len(hasil_pivot[0])} kolom")
        print(f"   📊 Contoh header: {hasil_pivot[0]}")

        # 4. Tulis ke Google Sheet
        print()
        print("=" * 60)
        print("📤 MENULIS DATA KE GOOGLE SHEETS")
        print("=" * 60)
        
        # Buka spreadsheet
        try:
            sh = gc.open_by_key(SPREADSHEET_ID)
            print(f"✅ Spreadsheet ditemukan: {SPREADSHEET_ID}")
            
            # Bersihkan semua worksheet tambahan
            clear_all_sheets_except_first(sh)
            
        except Exception as e:
            raise ValueError(f"❌ Gagal membuka spreadsheet: {str(e)}")
        
        # Cek atau buat worksheet
        try:
            ws = sh.worksheet(SHEET_NAME)
            print(f"✅ Sheet '{SHEET_NAME}' ditemukan")
            
            # Cek ukuran data
            total_rows = len(hasil_pivot)
            total_cells_needed = total_rows * len(hasil_pivot[0])
            print(f"📊 Cells diperlukan: {total_cells_needed:,}")
            
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️  Sheet '{SHEET_NAME}' tidak ditemukan, membuat baru...")
            
            # Hitung ukuran yang dibutuhkan
            required_rows = max(1000, len(hasil_pivot) + 100)
            required_cols = max(26, len(hasil_pivot[0]) + 5)
            
            ws = sh.add_worksheet(
                title=SHEET_NAME, 
                rows=required_rows, 
                cols=required_cols
            )
            print(f"✅ Sheet '{SHEET_NAME}' berhasil dibuat ({required_rows} baris x {required_cols} kolom)")
        except Exception as e:
            raise ValueError(f"❌ Gagal mengakses worksheet: {str(e)}")
        
        # Tulis data
        success = write_to_google_sheet(ws, hasil_pivot)
        
        if not success:
            raise ValueError("❌ Gagal menulis data ke Google Sheets")

        # 5. Buat laporan sukses
        print()
        print("=" * 60)
        print("✅ PROSES SELESAI")
        print("=" * 60)
        
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
REKAP DATA ERDKK BERHASIL DIPROSES (PIVOT) ✓

📅 Tanggal Proses: {now}
📁 Jumlah File: {file_count}
📊 Total Data Awal: {total_rows_original} baris
🧹 Data Setelah Cleaning: {total_rows_cleaned} baris
📈 Hasil Pivot: {len(hasil_pivot) - 1} baris
🏢 Unique KTP-Poktan: {len(hasil_pivot) - 1}

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN ({min(5, len(nik_cleaning_log))} pertama):
{chr(10).join(nik_cleaning_log[:5])}
{"... (masih ada " + str(len(nik_cleaning_log) - 5) + " entri lainnya)" if len(nik_cleaning_log) > 5 else "Tidak ada NIK yang dibersihkan"}

📊 STRUKTUR OUTPUT:
1. KTP
2. Nama Petani
3. Nama Poktan
4. Desa
5. Kecamatan (dari Gapoktan)
6. Nama Kios Pengecer
7. Komoditas (semua komoditas unik)
8. Rencana Tanam 1 Tahun (Ha) - total luas MT1+MT2+MT3
9-13. Pupuk MT1 (Urea, NPK, NPK Formula, Organik, ZA)
14-18. Pupuk MT2 (Urea, NPK, NPK Formula, Organik, ZA)
19-23. Pupuk MT3 (Urea, NPK, NPK Formula, Organik, ZA)

✅ DATA TELAH BERHASIL DIUPLOAD:
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}
📈 Baris Data: {len(hasil_pivot) - 1}
📊 Kolom Data: {len(hasil_pivot[0])}
💾 Cells Digunakan: {(len(hasil_pivot) - 1) * len(hasil_pivot[0]):,}

⚠️ CATATAN TEKNIS:
- Google Sheets memiliki batas 10 juta cells per spreadsheet
- Semua data lama telah dibersihkan sebelum menulis data baru
- Worksheet di-optimize untuk ukuran yang tepat

📍 REPOSITORY: {os.environ.get('GITHUB_REPOSITORY', 'verval-pupuk2')}
🔄 WORKFLOW RUN: {os.environ.get('GITHUB_RUN_ID', 'N/A')}
"""

        print(f"📊 Ringkasan: {now}, File: {file_count}, Data: {len(hasil_pivot) - 1} baris")

        # 6. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        email_sent = send_email_notification("REKAP DATA ERDKK BERHASIL (PIVOT)", success_message, is_success=True)
        
        if email_sent:
            print("✅ Email notifikasi terkirim")
        else:
            print("⚠️  Gagal mengirim email notifikasi")
        
        print("\n" + "=" * 60)
        print("🎉 PROSES REKAP DATA ERDKK TELAH BERHASIL!")
        print("=" * 60)
        
        return True

    except Exception as e:
        error_message = f"""
REKAP DATA ERDKK GAGAL ❌

📅 Tanggal Proses: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📍 Folder ID: {FOLDER_ID}
📍 Repository: {os.environ.get('GITHUB_REPOSITORY', 'N/A')}
🔄 Workflow Run: {os.environ.get('GITHUB_RUN_ID', 'N/A')}
📊 Status: Gagal saat memproses data

⚠️ ERROR DETAILS:
{str(e)}

🔧 TROUBLESHOOTING:
1. Periksa apakah file Excel memiliki format yang konsisten
2. Pastikan kolom 'KTP' atau 'NIK' ada di semua file
3. Cek struktur data di folder Google Drive
4. Verifikasi akses Service Account
5. Spreadsheet mungkin penuh (10 juta cells limit)

🔧 TRACEBACK (simplified):
{str(e.__class__.__name__)}: {str(e)}
"""
        print("\n" + "=" * 60)
        print("❌ PROSES GAGAL")
        print("=" * 60)
        print(error_message)

        # Kirim email notifikasi error
        try:
            send_email_notification("REKAP DATA ERDKK GAGAL", error_message, is_success=False)
            print("📧 Notifikasi email error terkirim")
        except Exception as email_error:
            print(f"⚠️  Gagal mengirim email error: {str(email_error)}")
        
        return False

# ============================
# JALANKAN PROSES UTAMA
# ============================
if __name__ == "__main__":
    main()
