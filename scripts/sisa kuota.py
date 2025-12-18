import os
import pandas as pd
import numpy as np
import gspread
import re
import json
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import tempfile

# ============================
# KONFIGURASI
# ============================
ERDKK_FOLDER_ID = "1BBgVsgq7EMGs0TLaO_4GEtUppznm1v5J"
REALISASI_FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1GRo7BP1a2MdEjZEnUVVDQBqp7KuLOViRlxMOkL0ERJE/edit"

# ============================
# EMAIL CONFIG (DARI SECRETS)
# ============================
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")  # dipisah koma

EMAIL_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": SENDER_EMAIL,
    "sender_password": SENDER_EMAIL_PASSWORD,
    "recipient_emails": [email.strip() for email in RECIPIENT_EMAILS.split(",")] if RECIPIENT_EMAILS else []
}

# ============================
# FUNGSI UTILITY - TIDAK BERUBAH
# ============================
def clean_nik(nik_value):
    """Membersihkan NIK dari karakter non-angka"""
    if pd.isna(nik_value) or nik_value is None:
        return None

    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)

    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")

    return cleaned_nik if cleaned_nik else None

def clean_kode_kios(kode_value):
    """Membersihkan kode kios dengan konsisten"""
    if pd.isna(kode_value) or kode_value is None:
        return ''
    
    kode_str = str(kode_value)
    
    # 1. Hapus whitespace berlebih
    kode_cleaned = ' '.join(kode_str.strip().split())
    
    # 2. Hapus karakter khusus kecuali huruf, angka, spasi, dan dash
    kode_cleaned = re.sub(r'[^\w\s-]', '', kode_cleaned)
    
    # 3. Uppercase
    kode_cleaned = kode_cleaned.upper()
    
    # 4. Hapus leading/trailing whitespace lagi
    kode_cleaned = kode_cleaned.strip()
    
    return kode_cleaned

def send_email_notification(subject, message, is_success=True):
    """Mengirim notifikasi email (menggunakan secrets/env)"""
    try:
        # Validasi konfigurasi email
        if (
            not EMAIL_CONFIG.get("sender_email")
            or not EMAIL_CONFIG.get("sender_password")
            or not EMAIL_CONFIG.get("recipient_emails")
        ):
            print("❌ Konfigurasi email belum lengkap (cek secrets)")
            return False

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
                    <p>
                        <small>
                            Dikirim secara otomatis pada
                            {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
                        </small>
                    </p>
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
                    <p>
                        <small>
                            Dikirim secara otomatis pada
                            {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
                        </small>
                    </p>
                </body>
            </html>
            """

        msg.attach(MIMEText(email_body, 'html'))

        with smtplib.SMTP(
            EMAIL_CONFIG["smtp_server"],
            EMAIL_CONFIG["smtp_port"]
        ) as server:
            server.starttls()
            server.login(
                EMAIL_CONFIG["sender_email"],
                EMAIL_CONFIG["sender_password"]
            )
            server.send_message(msg)

        print(f"📧 Email terkirim ke {EMAIL_CONFIG['recipient_emails'][0]}")
        return True

    except Exception as e:
        print(f"❌ Gagal mengirim email: {str(e)}")
        return False


# ============================
# FUNGSI DOWNLOAD FILE - TIDAK BERUBAH
# ============================
def download_excel_files(credentials, folder_id, folder_name):
    """Download file Excel dari Google Drive ke temporary folder"""
    temp_dir = tempfile.gettempdir()
    save_folder = os.path.join(temp_dir, f"data_{folder_name}_{int(time.time())}")
    os.makedirs(save_folder, exist_ok=True)

    drive_service = build('drive', 'v3', credentials=credentials)

    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        print(f"⚠️  Tidak ada file Excel di folder {folder_name}")
        return []

    file_paths = []
    for file in files:
        print(f"📥 Downloading {folder_name}: {file['name']}")
        request = drive_service.files().get_media(fileId=file["id"])
        file_path = os.path.join(save_folder, file["name"])

        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        file_paths.append({
            'path': file_path,
            'name': file['name'],
            'temp_folder': save_folder
        })

    print(f"✅ Berhasil download {len(file_paths)} file dari {folder_name} ke {save_folder}")
    return file_paths

# ============================
# FUNGSI PROSES DATA ERDKK - DIPERBAIKI (SHEET1)
# ============================
def process_single_erdkk_row(row, file_name=""):
    """Proses satu baris data ERDKK dengan validasi lebih ketat"""
    result = {
        'NIK': None,
        'NAMA_PETANI': '',
        'KODE_KIOS': '',
        'NAMA_KIOS': '',
        'TOTAL_UREA': 0,
        'TOTAL_NPK': 0,
        'TOTAL_SP36': 0,
        'TOTAL_ZA': 0,
        'TOTAL_NPK_FORMULA': 0,
        'TOTAL_ORGANIK': 0,
        'TOTAL_ORGANIK_CAIR': 0,
        'FILE_SOURCE': file_name
    }

    try:
        # Clean NIK
        nik = clean_nik(row.get('KTP', ''))
        if not nik:
            return None

        result['NIK'] = nik
        result['NAMA_PETANI'] = str(row.get('Nama Petani', '')).strip()

        # Ambil kode kios dari ERDKK
        kode_kios_raw = row.get('Kode Kios Pengecer', '')
        nama_kios_raw = row.get('Nama Kios Pengecer', '')

        # Clean kode kios dengan fungsi khusus
        result['KODE_KIOS'] = clean_kode_kios(kode_kios_raw)
        result['NAMA_KIOS'] = str(nama_kios_raw).strip() if pd.notna(nama_kios_raw) else ''

        # Hitung total pupuk per jenis (MT1 + MT2 + MT3)
        for mt in ['MT1', 'MT2', 'MT3']:
            # Urea
            urea_col = f'Pupuk Urea (Kg) {mt}'
            if urea_col in row:
                value = pd.to_numeric(row[urea_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_UREA'] += float(value)

            # NPK
            npk_col = f'Pupuk NPK (Kg) {mt}'
            if npk_col in row:
                value = pd.to_numeric(row[npk_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_NPK'] += float(value)

            # SP36
            sp36_col = f'Pupuk SP36 (Kg) {mt}'
            if sp36_col in row:
                value = pd.to_numeric(row[sp36_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_SP36'] += float(value)

            # ZA
            za_col = f'Pupuk ZA (Kg) {mt}'
            if za_col in row:
                value = pd.to_numeric(row[za_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_ZA'] += float(value)

            # NPK Formula
            npk_formula_col = f'Pupuk NPK Formula (Kg) {mt}'
            if npk_formula_col in row:
                value = pd.to_numeric(row[npk_formula_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_NPK_FORMULA'] += float(value)

            # Organik
            organik_col = f'Pupuk Organik (Kg) {mt}'
            if organik_col in row:
                value = pd.to_numeric(row[organik_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_ORGANIK'] += float(value)

            # Organik Cair (jika ada)
            organik_cair_col = f'Pupuk Organik Cair (Kg) {mt}'
            if organik_cair_col in row:
                value = pd.to_numeric(row[organik_cair_col], errors='coerce')
                if pd.notna(value):
                    result['TOTAL_ORGANIK_CAIR'] += float(value)

        # Cek apakah ada data pupuk yang valid
        pupuk_total = (result['TOTAL_UREA'] + result['TOTAL_NPK'] + result['TOTAL_SP36'] + 
                      result['TOTAL_ZA'] + result['TOTAL_NPK_FORMULA'] + 
                      result['TOTAL_ORGANIK'] + result['TOTAL_ORGANIK_CAIR'])
        
        if pupuk_total <= 0:
            return None  # Skip jika tidak ada pupuk

        # Bulatkan nilai
        for key in ['TOTAL_UREA', 'TOTAL_NPK', 'TOTAL_SP36', 'TOTAL_ZA', 
                   'TOTAL_NPK_FORMULA', 'TOTAL_ORGANIK', 'TOTAL_ORGANIK_CAIR']:
            result[key] = round(result[key], 2)

        return result

    except Exception as e:
        print(f"   ⚠️  Error processing row: {e}")
        return None

def process_erdkk_file(file_path, file_name):
    """Proses satu file ERDKK - SHEET DIPERBAIKI MENJADI Sheet1"""
    try:
        print(f"\n   📖 Memproses ERDKK: {file_name}")

        # Coba beberapa opsi untuk membaca file ERDKK
        sheet_options = ['Sheet1', 'SHEET1', 'sheet1', 'Worksheet', 'WORKSHEET', 'worksheet']
        
        df = None
        used_sheet = None
        
        for sheet_name in sheet_options:
            try:
                print(f"   🔍 Mencoba sheet: '{sheet_name}'")
                df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                used_sheet = sheet_name
                print(f"   ✅ Berhasil membaca dengan sheet: '{sheet_name}'")
                break
            except Exception as e:
                print(f"   ❌ Gagal membaca sheet '{sheet_name}': {e}")
                continue
        
        # Jika masih gagal, coba baca sheet pertama
        if df is None:
            try:
                print(f"   🔍 Mencoba sheet pertama (index 0)")
                df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                used_sheet = "sheet pertama (index 0)"
                print(f"   ✅ Berhasil membaca sheet pertama")
            except Exception as e:
                print(f"   ❌ Gagal membaca sheet pertama: {e}")
                return []
        
        print(f"   📊 Sheet yang digunakan: {used_sheet}")
        print(f"   📊 Dimensi data: {df.shape[0]} baris x {df.shape[1]} kolom")
        
        # Standardize column names
        df.columns = df.columns.str.strip()
        print(f"   📋 Kolom setelah cleaning: {list(df.columns)}")
        
        # Cari kolom KTP (mungkin ada variasi penulisan)
        ktp_columns = [col for col in df.columns if 'KTP' in col.upper() or 'NIK' in col.upper()]
        if ktp_columns:
            print(f"   🔍 Kolom KTP/NIK ditemukan: {ktp_columns}")
            # Gunakan kolom pertama yang ditemukan
            df = df.rename(columns={ktp_columns[0]: 'KTP'})
        
        # Cari kolom Nama Petani
        nama_columns = [col for col in df.columns if 'NAMA' in col.upper() and 'PETANI' in col.upper()]
        if nama_columns:
            print(f"   🔍 Kolom Nama Petani ditemukan: {nama_columns}")
            df = df.rename(columns={nama_columns[0]: 'Nama Petani'})
        
        # Cari kolom Kode Kios
        kode_columns = [col for col in df.columns if 'KODE' in col.upper() and 'KIOS' in col.upper()]
        if kode_columns:
            print(f"   🔍 Kolom Kode Kios ditemukan: {kode_columns}")
            df = df.rename(columns={kode_columns[0]: 'Kode Kios Pengecer'})
        
        # Cari kolom Nama Kios
        nama_kios_columns = [col for col in df.columns if 'NAMA' in col.upper() and 'KIOS' in col.upper()]
        if nama_kios_columns:
            print(f"   🔍 Kolom Nama Kios ditemukan: {nama_kios_columns}")
            df = df.rename(columns={nama_kios_columns[0]: 'Nama Kios Pengecer'})
        
        # Cek kolom wajib setelah renaming
        required_cols = ['KTP', 'Nama Petani', 'Kode Kios Pengecer', 'Nama Kios Pengecer']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"   ⚠️  Kolom tidak ditemukan: {missing_cols}")
            print(f"   🔍 Kolom yang ada: {list(df.columns)}")
            
            # Coba cari kolom dengan pattern matching
            all_cols_upper = [col.upper() for col in df.columns]
            for req_col in missing_cols:
                req_pattern = req_col.upper().replace(' ', '.*')
                matching = [col for col in df.columns if re.search(req_pattern, col.upper())]
                if matching:
                    print(f"   🔍 Pattern match untuk '{req_col}': {matching}")
        
        # DEBUG: Tampilkan beberapa baris pertama untuk inspeksi
        if len(df) > 0:
            print(f"\n   🔍 Sample 3 baris pertama (kolom terpilih):")
            sample_cols = []
            for col in ['KTP', 'Nama Petani', 'Kode Kios Pengecer', 'Nama Kios Pengecer', 
                       'Pupuk Urea (Kg) MT1', 'Pupuk NPK (Kg) MT1', 'Pupuk Organik (Kg) MT1']:
                if col in df.columns:
                    sample_cols.append(col)
            
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                print(f"   Baris {i+1}:")
                for col in sample_cols:
                    val = row[col]
                    if pd.notna(val):
                        print(f"     {col}: {str(val)[:30]}...")
                    else:
                        print(f"     {col}: (kosong)")

        results = []

        # Proses setiap baris
        for idx, row in df.iterrows():
            row_result = process_single_erdkk_row(row, file_name)

            if row_result:
                results.append(row_result)

        print(f"   ✅ Berhasil: {len(results)} baris data")
        
        # DEBUG: Tampilkan sample data
        if results and len(results) > 0:
            print(f"   🔍 Sample data ERDKK (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     Nama: {sample['NAMA_PETANI'][:20]}...")
            print(f"     Kode Kios: '{sample['KODE_KIOS']}'")
            print(f"     Urea: {sample['TOTAL_UREA']}")
            print(f"     Organik: {sample['TOTAL_ORGANIK']}")

        return results

    except Exception as e:
        print(f"   ❌ Error memproses ERDKK {file_name}: {str(e)}")
        traceback.print_exc()
        return []

def pivot_erdkk_data(all_erdkk_rows):
    """Pivot data ERDKK berdasarkan NIK dan KODE_KIOS dengan duplikasi handling"""
    if not all_erdkk_rows:
        return pd.DataFrame()

    print("\n📊 Membuat pivot data ERDKK...")

    # Convert to DataFrame
    df = pd.DataFrame(all_erdkk_rows)
    
    # Debug: Tampilkan duplikasi sebelum pivot
    duplicate_check = df.duplicated(subset=['NIK', 'KODE_KIOS'], keep=False)
    if duplicate_check.any():
        duplicates = df[duplicate_check]
        print(f"   ⚠️  Ditemukan {len(duplicates)} baris duplikat (NIK + KODE_KIOS) sebelum pivot")
        print(f"   🔍 Sample duplikat:")
        print(duplicates[['NIK', 'KODE_KIOS', 'TOTAL_UREA', 'TOTAL_ORGANIK']].head(5).to_string())

    # Group by NIK dan KODE_KIOS
    group_cols = ['NIK', 'KODE_KIOS', 'NAMA_PETANI', 'NAMA_KIOS']

    # Pastikan semua kolom ada
    for col in group_cols:
        if col not in df.columns:
            df[col] = ''

    # Aggregation dictionary
    agg_dict = {
        'NAMA_PETANI': 'first',
        'NAMA_KIOS': 'first',
        'TOTAL_UREA': 'sum',
        'TOTAL_NPK': 'sum',
        'TOTAL_SP36': 'sum',
        'TOTAL_ZA': 'sum',
        'TOTAL_NPK_FORMULA': 'sum',
        'TOTAL_ORGANIK': 'sum',
        'TOTAL_ORGANIK_CAIR': 'sum'
    }

    # Group data
    pivoted_df = df.groupby(['NIK', 'KODE_KIOS']).agg(agg_dict).reset_index()

    # Debug: Tampilkan duplikasi setelah pivot
    print(f"\n   📊 Statistik setelah pivot:")
    print(f"      • Baris sebelum pivot: {len(df)}")
    print(f"      • Baris setelah pivot: {len(pivoted_df)}")
    print(f"      • Pengurangan: {len(df) - len(pivoted_df)} baris digabung")

    # Round values
    pupuk_cols = ['TOTAL_UREA', 'TOTAL_NPK', 'TOTAL_SP36', 'TOTAL_ZA', 
                  'TOTAL_NPK_FORMULA', 'TOTAL_ORGANIK', 'TOTAL_ORGANIK_CAIR']

    for col in pupuk_cols:
        if col in pivoted_df.columns:
            pivoted_df[col] = pivoted_df[col].round(2)

    print(f"\n✅ Pivot selesai: {len(pivoted_df)} baris")

    return pivoted_df

# ============================
# FUNGSI PROSES DATA REALISASI - TIDAK BERUBAH
# ============================
def get_manual_mapping_for_realisasi(file_name):
    """Mapping manual berdasarkan format header yang diketahui"""
    print(f"   🔧 Menggunakan mapping manual untuk format realisasi")
    
    return {
        'nik_col': 'NIK',
        'nama_col': 'NAMA PETANI',
        'kode_kios_col': 'KODE KIOS',
        'nama_kios_col': 'NAMA KIOS',
        'kecamatan_col': 'KECAMATAN',
        'pupuk_cols': {
            'urea': 'UREA',
            'npk': 'NPK',
            'sp36': 'SP36',
            'za': 'ZA',
            'npk_formula': 'NPK FORMULA',
            'organik': 'ORGANIK',
            'organik_cair': 'ORGANIK CAIR'
        }
    }

def clean_column_name(col_name):
    """Bersihkan nama kolom"""
    if pd.isna(col_name):
        return ""
    
    col_str = str(col_name)
    col_clean = col_str.strip().upper()
    col_clean = re.sub(r'\s+', ' ', col_clean)
    
    return col_clean

def process_single_realisasi_row(row, column_mapping, file_name=""):
    """Proses satu baris data realisasi dengan cleaning yang konsisten"""
    result = {
        'NIK': None,
        'NAMA_PETANI': '',
        'KODE_KIOS': '',
        'NAMA_KIOS': '',
        'KECAMATAN': '',
        'REALISASI_UREA': 0,
        'REALISASI_NPK': 0,
        'REALISASI_SP36': 0,
        'REALISASI_ZA': 0,
        'REALISASI_NPK_FORMULA': 0,
        'REALISASI_ORGANIK': 0,
        'REALISASI_ORGANIK_CAIR': 0,
        'FILE_SOURCE': file_name
    }

    try:
        # 1. AMBIL NIK (wajib)
        nik_col = column_mapping.get('nik_col')
        if nik_col and nik_col in row:
            nik_raw = row[nik_col]
            nik = clean_nik(nik_raw)
            if not nik:
                return None  # Baris tanpa NIK di-skip
            result['NIK'] = nik
        
        if not result['NIK']:
            return None  # Skip jika tidak ada NIK
        
        # 2. AMBIL NAMA PETANI
        nama_col = column_mapping.get('nama_col')
        if nama_col and nama_col in row:
            nama_raw = row[nama_col]
            if pd.notna(nama_raw):
                result['NAMA_PETANI'] = str(nama_raw).strip()
        
        # 3. AMBIL KODE KIOS dengan cleaning yang sama seperti ERDKK
        kode_kios_col = column_mapping.get('kode_kios_col')
        if kode_kios_col and kode_kios_col in row:
            kode_raw = row[kode_kios_col]
            result['KODE_KIOS'] = clean_kode_kios(kode_raw)
        
        # 4. AMBIL NAMA KIOS
        nama_kios_col = column_mapping.get('nama_kios_col')
        if nama_kios_col and nama_kios_col in row:
            nama_kios_raw = row[nama_kios_col]
            if pd.notna(nama_kios_raw):
                result['NAMA_KIOS'] = str(nama_kios_raw).strip()
        
        # 5. AMBIL KECAMATAN
        kecamatan_col = column_mapping.get('kecamatan_col')
        if kecamatan_col and kecamatan_col in row:
            kecamatan_raw = row[kecamatan_col]
            if pd.notna(kecamatan_raw):
                result['KECAMATAN'] = str(kecamatan_raw).strip().upper()
        
        # 6. AMBIL DATA PUPUK
        pupuk_mapping = {
            'urea': 'REALISASI_UREA',
            'npk': 'REALISASI_NPK',
            'sp36': 'REALISASI_SP36',
            'za': 'REALISASI_ZA',
            'npk_formula': 'REALISASI_NPK_FORMULA',
            'organik': 'REALISASI_ORGANIK',
            'organik_cair': 'REALISASI_ORGANIK_CAIR'
        }
        
        pupuk_cols_dict = column_mapping.get('pupuk_cols', {})
        
        for pupuk_type, result_key in pupuk_mapping.items():
            if pupuk_type in pupuk_cols_dict:
                col_name = pupuk_cols_dict[pupuk_type]
                if col_name in row:
                    raw_value = row[col_name]
                    
                    if pd.notna(raw_value):
                        try:
                            # Convert ke numeric dengan handling yang kuat
                            value = pd.to_numeric(raw_value, errors='coerce')
                            if pd.notna(value):
                                result[result_key] = float(value)
                        except:
                            pass
        
        # 7. Cek apakah ada data pupuk yang valid
        has_pupuk_data = any([
            result['REALISASI_UREA'] > 0,
            result['REALISASI_NPK'] > 0,
            result['REALISASI_SP36'] > 0,
            result['REALISASI_ZA'] > 0,
            result['REALISASI_NPK_FORMULA'] > 0,
            result['REALISASI_ORGANIK'] > 0,
            result['REALISASI_ORGANIK_CAIR'] > 0
        ])
        
        if not has_pupuk_data:
            return None

        # Bulatkan nilai
        for key in ['REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 'REALISASI_ZA',
                   'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']:
            result[key] = round(result[key], 2)

        return result

    except Exception as e:
        print(f"   ⚠️  Error processing realisasi row: {e}")
        return None

def process_realisasi_file(file_path, file_name):
    """Proses satu file realisasi dengan mapping manual"""
    try:
        print(f"\n   📖 Memproses Realisasi: {file_name}")

        # Baca file Excel
        try:
            df = pd.read_excel(file_path, dtype=str)
        except Exception as e1:
            try:
                df = pd.read_excel(file_path, header=1, dtype=str)
            except Exception as e2:
                try:
                    df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
                except Exception as e3:
                    print(f"   ❌ Gagal membaca file: {e3}")
                    return []

        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Gunakan mapping manual
        column_mapping = get_manual_mapping_for_realisasi(file_name)
        
        results = []
        processed_count = 0
        skipped_count = 0
        
        # Proses setiap baris
        for idx, row in df.iterrows():
            row_result = process_single_realisasi_row(row, column_mapping, file_name)

            if row_result:
                results.append(row_result)
                processed_count += 1
            else:
                skipped_count += 1

        print(f"\n   📊 Statistik pemrosesan:")
        print(f"      • Total baris dalam file: {len(df)}")
        print(f"      • Berhasil diproses: {processed_count}")
        print(f"      • Dilewati: {skipped_count}")
        
        # DEBUG: Tampilkan sample data
        if results and len(results) > 0:
            print(f"   🔍 Sample data Realisasi (baris pertama):")
            sample = results[0]
            print(f"     NIK: {sample['NIK']}")
            print(f"     Nama: {sample['NAMA_PETANI'][:20]}...")
            print(f"     Kode Kios: '{sample['KODE_KIOS']}'")
            print(f"     Urea: {sample['REALISASI_UREA']}")
            print(f"     Organik: {sample['REALISASI_ORGANIK']}")

        return results

    except Exception as e:
        print(f"   ❌ Error memproses realisasi {file_name}: {str(e)}")
        traceback.print_exc()
        return []

def pivot_realisasi_data(all_realisasi_rows):
    """Pivot data realisasi berdasarkan NIK dan KODE_KIOS dengan duplikasi handling"""
    if not all_realisasi_rows:
        return pd.DataFrame()

    print("\n📊 Membuat pivot data realisasi...")

    # Convert to DataFrame
    df = pd.DataFrame(all_realisasi_rows)
    
    # Debug: Tampilkan duplikasi sebelum pivot
    duplicate_check = df.duplicated(subset=['NIK', 'KODE_KIOS'], keep=False)
    if duplicate_check.any():
        duplicates = df[duplicate_check]
        print(f"   ⚠️  Ditemukan {len(duplicates)} baris duplikat (NIK + KODE_KIOS) sebelum pivot")

    # Pastikan semua kolom ada
    group_cols = ['NIK', 'KODE_KIOS', 'NAMA_PETANI', 'NAMA_KIOS', 'KECAMATAN']
    for col in group_cols:
        if col not in df.columns:
            df[col] = ''

    # Aggregation dictionary
    agg_dict = {
        'NAMA_PETANI': 'first',
        'NAMA_KIOS': 'first',
        'KECAMATAN': 'first',
        'REALISASI_UREA': 'sum',
        'REALISASI_NPK': 'sum',
        'REALISASI_SP36': 'sum',
        'REALISASI_ZA': 'sum',
        'REALISASI_NPK_FORMULA': 'sum',
        'REALISASI_ORGANIK': 'sum',
        'REALISASI_ORGANIK_CAIR': 'sum'
    }
    
    # Group data
    pivoted_df = df.groupby(['NIK', 'KODE_KIOS']).agg(agg_dict).reset_index()
    
    # Debug: Tampilkan duplikasi setelah pivot
    print(f"\n   📊 Statistik setelah pivot:")
    print(f"      • Baris sebelum pivot: {len(df)}")
    print(f"      • Baris setelah pivot: {len(pivoted_df)}")
    print(f"      • Pengurangan: {len(df) - len(pivoted_df)} baris digabung")
    
    # Round values
    pupuk_cols = ['REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 'REALISASI_ZA', 
                  'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']
    
    for col in pupuk_cols:
        if col in pivoted_df.columns:
            pivoted_df[col] = pivoted_df[col].round(2)
    
    print(f"\n✅ Pivot realisasi selesai: {len(pivoted_df)} baris")
    
    return pivoted_df

# ============================
# FUNGSI HITUNG SISA - DIPERBAIKI DENGAN DEBUG
# ============================
def calculate_sisa_data(kuota_df, realisasi_df):
    """Hitung sisa pupuk (Kuota - Realisasi) dengan debugging detail"""
    print("\n🧮 Menghitung sisa pupuk (Kuota - Realisasi)...")
    
    # Debug: Tampilkan beberapa baris sebelum perhitungan
    print(f"\n   🔍 DATA KUOTA (5 baris pertama):")
    print(kuota_df[['NIK', 'KODE_KIOS', 'KUOTA_UREA', 'KUOTA_ORGANIK']].head().to_string())
    
    if realisasi_df is not None and not realisasi_df.empty:
        print(f"\n   🔍 DATA REALISASI (5 baris pertama):")
        print(realisasi_df[['NIK', 'KODE_KIOS', 'REALISASI_UREA', 'REALISASI_ORGANIK']].head().to_string())
    
    # Cek duplikasi sebelum merge
    print(f"\n   📊 Cek duplikasi kunci merge:")
    print(f"      • Kuota unique keys: {kuota_df[['NIK', 'KODE_KIOS']].drop_duplicates().shape[0]}")
    
    if realisasi_df is not None and not realisasi_df.empty:
        print(f"      • Realisasi unique keys: {realisasi_df[['NIK', 'KODE_KIOS']].drop_duplicates().shape[0]}")
    
    # Buat kunci merge yang konsisten
    kuota_df["MERGE_KEY"] = kuota_df["NIK"].astype(str).str.strip() + "||" + kuota_df["KODE_KIOS"].astype(str).str.strip()
    
    if realisasi_df is not None and not realisasi_df.empty:
        realisasi_df["MERGE_KEY"] = realisasi_df["NIK"].astype(str).str.strip() + "||" + realisasi_df["KODE_KIOS"].astype(str).str.strip()
    
    # Cek nilai NIK spesifik yang bermasalah
    print(f"\n   🔍 Cek NIK yang bermasalah:")
    
    # NIK 1104090705890001
    nik1 = "1104090705890001"
    kuota_nik1 = kuota_df[kuota_df["NIK"] == nik1]
    print(f"      • NIK {nik1} di kuota: {len(kuota_nik1)} baris")
    if not kuota_nik1.empty:
        print(f"        Kuota: Urea={kuota_nik1['KUOTA_UREA'].values[0]}")
    
    if realisasi_df is not None and not realisasi_df.empty:
        realisasi_nik1 = realisasi_df[realisasi_df["NIK"] == nik1]
        print(f"      • NIK {nik1} di realisasi: {len(realisasi_nik1)} baris")
        if not realisasi_nik1.empty:
            print(f"        Realisasi: Urea={realisasi_nik1['REALISASI_UREA'].values[0]}")
    
    # NIK 3509050602840004
    nik2 = "3509050602840004"
    kuota_nik2 = kuota_df[kuota_df["NIK"] == nik2]
    print(f"      • NIK {nik2} di kuota: {len(kuota_nik2)} baris")
    if not kuota_nik2.empty:
        print(f"        Kuota: Organik={kuota_nik2['KUOTA_ORGANIK'].values[0]}")
    
    if realisasi_df is not None and not realisasi_df.empty:
        realisasi_nik2 = realisasi_df[realisasi_df["NIK"] == nik2]
        print(f"      • NIK {nik2} di realisasi: {len(realisasi_nik2)} baris")
        if not realisasi_nik2.empty:
            print(f"        Realisasi: Organik={realisasi_nik2['REALISASI_ORGANIK'].values[0]}")
    
    # Jika realisasi kosong, semua sisa = kuota
    if realisasi_df is None or realisasi_df.empty:
        print("⚠️ Tidak ada realisasi, semua sisa = kuota.")
        sisa_df = kuota_df.copy()
        
        # Ganti nama kolom
        rename_map = {}
        for p in ['UREA', 'NPK', 'SP36', 'ZA', 'NPK_FORMULA', 'ORGANIK', 'ORGANIK_CAIR']:
            if f'KUOTA_{p}' in sisa_df.columns:
                rename_map[f'KUOTA_{p}'] = f'SISA_{p}'
        
        sisa_df = sisa_df.rename(columns=rename_map)
        
    else:
        # Merge dengan MERGE_KEY yang konsisten
        merged = kuota_df.merge(
            realisasi_df[['MERGE_KEY', 'REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 
                         'REALISASI_ZA', 'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']],
            on="MERGE_KEY",
            how="left"
        )
        
        print(f"\n   🔍 Setelah merge:")
        print(f"      • Total baris setelah merge: {len(merged)}")
        
        # Hitung sisa
        pupuk_types = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK_FORMULA', 'ORGANIK', 'ORGANIK_CAIR']
        
        for p in pupuk_types:
            kuota_col = f"KUOTA_{p}"
            real_col = f"REALISASI_{p}"
            sisa_col = f"SISA_{p}"
            
            if kuota_col in merged.columns:
                if real_col not in merged.columns:
                    merged[real_col] = 0
                
                merged[sisa_col] = merged[kuota_col].fillna(0) - merged[real_col].fillna(0)
                merged[sisa_col] = merged[sisa_col].round(2)
        
        # Debug: Tampilkan NIK bermasalah setelah merge
        print(f"\n   🔍 Setelah perhitungan sisa:")
        
        merged_nik1 = merged[merged["NIK"] == nik1]
        if not merged_nik1.empty:
            print(f"      • NIK {nik1}:")
            print(f"        Kuota Urea: {merged_nik1['KUOTA_UREA'].values[0]}")
            print(f"        Realisasi Urea: {merged_nik1['REALISASI_UREA'].values[0]}")
            print(f"        Sisa Urea: {merged_nik1['SISA_UREA'].values[0]}")
        
        merged_nik2 = merged[merged["NIK"] == nik2]
        if not merged_nik2.empty:
            print(f"      • NIK {nik2}:")
            print(f"        Kuota Organik: {merged_nik2['KUOTA_ORGANIK'].values[0]}")
            print(f"        Realisasi Organik: {merged_nik2['REALISASI_ORGANIK'].values[0]}")
            print(f"        Sisa Organik: {merged_nik2['SISA_ORGANIK'].values[0]}")
        
        # Pilih kolom output
        sisa_df = merged[['NIK', 'NAMA_PETANI', 'KODE_KIOS', 'NAMA_KIOS', 
                         'SISA_UREA', 'SISA_NPK', 'SISA_SP36', 'SISA_ZA',
                         'SISA_NPK_FORMULA', 'SISA_ORGANIK', 'SISA_ORGANIK_CAIR']].copy()
    
    print(f"\n✅ Perhitungan sisa selesai: {len(sisa_df)} baris")
    
    # Cek nilai negatif
    negative_count = 0
    for col in ['SISA_UREA', 'SISA_NPK', 'SISA_SP36', 'SISA_ZA', 
                'SISA_NPK_FORMULA', 'SISA_ORGANIK', 'SISA_ORGANIK_CAIR']:
        if col in sisa_df.columns:
            neg = (sisa_df[col] < 0).sum()
            if neg > 0:
                negative_count += neg
                print(f"   ⚠️  {col}: {neg} baris negatif")
    
    if negative_count > 0:
        print(f"   ⚠️  TOTAL: {negative_count} baris dengan nilai negatif ditemukan!")
        
        # Tampilkan 5 baris pertama dengan nilai negatif
        mask = (sisa_df['SISA_UREA'] < 0) | (sisa_df['SISA_NPK'] < 0) | (sisa_df['SISA_SP36'] < 0) | \
               (sisa_df['SISA_ZA'] < 0) | (sisa_df['SISA_NPK_FORMULA'] < 0) | \
               (sisa_df['SISA_ORGANIK'] < 0) | (sisa_df['SISA_ORGANIK_CAIR'] < 0)
        
        negatives = sisa_df[mask]
        print(f"\n   🔍 5 baris pertama dengan nilai negatif:")
        print(negatives.head().to_string())
    
    return sisa_df

# ============================
# FUNGSI CLEANUP TEMPORARY FILES - TIDAK BERUBAH
# ============================
def cleanup_temp_files(file_paths_list):
    """Hapus file temporary yang sudah tidak diperlukan"""
    print("\n🧹 Membersihkan file temporary...")
    
    deleted_count = 0
    error_count = 0
    
    for file_paths in file_paths_list:
        if not file_paths:
            continue
            
        for file_info in file_paths:
            try:
                if os.path.exists(file_info['path']):
                    os.remove(file_info['path'])
                    deleted_count += 1
                
                if 'temp_folder' in file_info and os.path.exists(file_info['temp_folder']):
                    try:
                        os.rmdir(file_info['temp_folder'])
                    except:
                        pass
                        
            except Exception as e:
                error_count += 1
                print(f"   ⚠️  Gagal menghapus {file_info['path']}: {e}")
    
    print(f"✅ Cleanup selesai: {deleted_count} file dihapus, {error_count} error")

# ============================
# FUNGSI UTAMA - DIPERBAIKI
# ============================
def update_or_create_single_sheet(gc, sheet_url, sheet_name, data_df):
    """Update atau buat hanya satu sheet (Sisa)"""
    try:
        spreadsheet = gc.open_by_url(sheet_url)
        
        existing_sheets = spreadsheet.worksheets()
        existing_sheet_names = [ws.title for ws in existing_sheets]
        
        print(f"📋 Sheets yang sudah ada: {existing_sheet_names}")
        
        # Hapus sheet lama jika ada
        if sheet_name in existing_sheet_names:
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"📝 Sheet '{sheet_name}' sudah ada, menghapus isi...")
                worksheet.clear()
            except Exception as e:
                print(f"⚠️  Gagal mengakses sheet '{sheet_name}': {e}, membuat baru...")
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=max(len(data_df) + 100, 1000), 
                    cols=len(data_df.columns) + 5
                )
        else:
            print(f"📝 Sheet '{sheet_name}' tidak ada, membuat baru...")
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, 
                rows=max(len(data_df) + 100, 1000), 
                cols=len(data_df.columns) + 5
            )
        
        print(f"📤 Mengupdate data ke sheet '{sheet_name}'...")
        
        # Update data
        data_values = [data_df.columns.values.tolist()] + data_df.values.tolist()
        worksheet.update(data_values)
        
        # Format header
        try:
            worksheet.format('A1:K1', {
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8},
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}}
            })
        except:
            pass
        
        print(f"✅ Sheet '{sheet_name}' berhasil diupdate: {len(data_df)} baris")
        return True
        
    except Exception as e:
        print(f"❌ Gagal update sheet '{sheet_name}': {str(e)}")
        return False

def process_step_by_step():
    """Fungsi utama dengan debugging detail"""
    print("=" * 60)
    print("🚀 MEMULAI PROSES DATA ERDKK & REALISASI - DIPERBAIKI")
    print("📋 ERDKK: Sheet1 | Realisasi: format standar")
    print("=" * 60)
    
    start_time = datetime.now()
    
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
        
        all_temp_files = []
        
        # ============================================
        # BAGIAN 1: PROSES DATA ERDKK (SHEET1)
        # ============================================
        print("\n" + "=" * 60)
        print("📋 BAGIAN 1: PROSES DATA ERDKK (Sheet1)")
        print("=" * 60)
        
        kuota_df = None
        
        erdkk_files = download_excel_files(credentials, ERDKK_FOLDER_ID, "erdkk")
        if erdkk_files:
            all_temp_files.append(erdkk_files)
        
        if not erdkk_files:
            print("⚠️  Tidak ada file ERDKK yang ditemukan")
        else:
            print(f"✅ Download selesai: {len(erdkk_files)} file")
            
            all_erdkk_rows = []
            processed_files = 0
            
            for file_info in erdkk_files:
                file_rows = process_erdkk_file(file_info['path'], file_info['name'])
                
                if file_rows:
                    all_erdkk_rows.extend(file_rows)
                    processed_files += 1
            
            if all_erdkk_rows:
                print(f"\n✅ Total file ERDKK diproses: {processed_files}/{len(erdkk_files)}")
                print(f"✅ Total baris data ERDKK: {len(all_erdkk_rows)}")
                
                pivoted_erdkk = pivot_erdkk_data(all_erdkk_rows)
                
                if not pivoted_erdkk.empty:
                    print(f"✅ Pivot data ERDKK selesai: {len(pivoted_erdkk)} baris")
                    
                    # Rename columns untuk kuota
                    kuota_df = pivoted_erdkk.rename(columns={
                        'TOTAL_UREA': 'KUOTA_UREA',
                        'TOTAL_NPK': 'KUOTA_NPK',
                        'TOTAL_SP36': 'KUOTA_SP36',
                        'TOTAL_ZA': 'KUOTA_ZA',
                        'TOTAL_NPK_FORMULA': 'KUOTA_NPK_FORMULA',
                        'TOTAL_ORGANIK': 'KUOTA_ORGANIK',
                        'TOTAL_ORGANIK_CAIR': 'KUOTA_ORGANIK_CAIR'
                    })
                    
                    # Urutkan kolom
                    final_erdkk_columns = ['NIK', 'NAMA_PETANI', 'KODE_KIOS', 'NAMA_KIOS',
                                          'KUOTA_UREA', 'KUOTA_NPK', 'KUOTA_SP36', 'KUOTA_ZA',
                                          'KUOTA_NPK_FORMULA', 'KUOTA_ORGANIK', 'KUOTA_ORGANIK_CAIR']
                    
                    kuota_df = kuota_df[final_erdkk_columns]
                    
                    # Tampilkan statistik
                    print(f"\n📊 Total Kuota Pupuk:")
                    for col in ['KUOTA_UREA', 'KUOTA_NPK', 'KUOTA_SP36', 'KUOTA_ZA',
                               'KUOTA_NPK_FORMULA', 'KUOTA_ORGANIK', 'KUOTA_ORGANIK_CAIR']:
                        if col in kuota_df.columns:
                            total = kuota_df[col].sum()
                            print(f"   • {col}: {total:,.2f} Kg")
        
        # ============================================
        # BAGIAN 2: PROSES DATA REALISASI
        # ============================================
        print("\n" + "=" * 60)
        print("📋 BAGIAN 2: PROSES DATA REALISASI")
        print("=" * 60)
        
        realisasi_df = None
        
        realisasi_files = download_excel_files(credentials, REALISASI_FOLDER_ID, "realisasi")
        if realisasi_files:
            all_temp_files.append(realisasi_files)
        
        if not realisasi_files:
            print("⚠️  Tidak ada file realisasi yang ditemukan")
        else:
            print(f"✅ Download selesai: {len(realisasi_files)} file")
            
            all_realisasi_rows = []
            processed_files = 0
            
            for file_info in realisasi_files:
                file_rows = process_realisasi_file(file_info['path'], file_info['name'])
                
                if file_rows:
                    all_realisasi_rows.extend(file_rows)
                    processed_files += 1
            
            if all_realisasi_rows:
                print(f"\n✅ Total file realisasi diproses: {processed_files}/{len(realisasi_files)}")
                print(f"✅ Total baris data realisasi: {len(all_realisasi_rows)}")
                
                pivoted_realisasi = pivot_realisasi_data(all_realisasi_rows)
                
                if not pivoted_realisasi.empty:
                    print(f"✅ Pivot data realisasi selesai: {len(pivoted_realisasi)} baris")
                    
                    realisasi_df = pivoted_realisasi
                    
                    # Tampilkan statistik
                    print(f"\n📊 Total Realisasi Pupuk:")
                    for col in ['REALISASI_UREA', 'REALISASI_NPK', 'REALISASI_SP36', 'REALISASI_ZA',
                               'REALISASI_NPK_FORMULA', 'REALISASI_ORGANIK', 'REALISASI_ORGANIK_CAIR']:
                        if col in realisasi_df.columns:
                            total = realisasi_df[col].sum()
                            print(f"   • {col}: {total:,.2f} Kg")
        
        # ============================================
        # BAGIAN 3: HITUNG SISA
        # ============================================
        print("\n" + "=" * 60)
        print("📋 BAGIAN 3: PERHITUNGAN SISA PUPUK")
        print("=" * 60)
        
        if kuota_df is None or kuota_df.empty:
            print("❌ Data kuota tidak tersedia, tidak dapat menghitung sisa")
            raise ValueError("Data kuota kosong")
        
        sisa_df = calculate_sisa_data(kuota_df, realisasi_df if realisasi_df is not None else pd.DataFrame())
        
        if not sisa_df.empty:
            print(f"\n✅ Data sisa berhasil dihitung: {len(sisa_df)} baris")
            
            # ============================================
            # BAGIAN 4: TULIS SHEET "SISA"
            # ============================================
            print("\n" + "=" * 60)
            print("📋 BAGIAN 4: MENULIS HASIL AKHIR KE SHEET 'SISA'")
            print("=" * 60)
            
            print("\n📤 Export data sisa ke Google Sheets...")
            update_or_create_single_sheet(gc, OUTPUT_SHEET_URL, "Sisa", sisa_df)
            
            # Tampilkan statistik akhir
            print(f"\n📊 STATISTIK AKHIR:")
            print(f"   • Total baris data sisa: {len(sisa_df)}")
            print(f"   • Total NIK unik: {sisa_df['NIK'].nunique()}")
            print(f"   • Total Kode Kios unik: {sisa_df['KODE_KIOS'].nunique()}")
            
            # Hitung total sisa pupuk
            sisa_cols = ['SISA_UREA', 'SISA_NPK', 'SISA_SP36', 'SISA_ZA',
                        'SISA_NPK_FORMULA', 'SISA_ORGANIK', 'SISA_ORGANIK_CAIR']
            
            for col in sisa_cols:
                if col in sisa_df.columns:
                    total = sisa_df[col].sum()
                    if total != 0:
                        print(f"   • {col}: {total:,.2f} Kg")
        
        # ============================================
        # BAGIAN 5: CLEANUP
        # ============================================
        print("\n" + "=" * 60)
        print("📋 BAGIAN 5: CLEANUP FILE TEMPORARY")
        print("=" * 60)
        
        cleanup_temp_files(all_temp_files)
        
        # ============================================
        # BAGIAN 6: SUMMARY
        # ============================================
        print("\n" + "=" * 60)
        print("📋 BAGIAN 6: SUMMARY HASIL")
        print("=" * 60)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Summary untuk email
        summary_message = f"""
SISA KUOTA BERHASIL ✓

⏰ Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

📊 HASIL:
- File ERDKK ditemukan: {len(erdkk_files) if 'erdkk_files' in locals() else 0}
- File Realisasi ditemukan: {len(realisasi_files) if 'realisasi_files' in locals() else 0}
- Baris kuota diproses: {len(kuota_df) if kuota_df is not None else 0}
- Baris sisa: {len(sisa_df) if 'sisa_df' in locals() and sisa_df is not None else 0}

📤 OUTPUT:
- Spreadsheet: {OUTPUT_SHEET_URL}
- Sheet: Sisa (Kuota - Realisasi)

✅ Semua proses intermediate dilakukan di memory/temporary.
✅ Hanya hasil akhir yang ditulis ke Google Sheets.
"""

        subject = "SISA KUOTA BERHASIL"
        send_email_notification(subject, summary_message, is_success=True)
        
        print(f"\n✅ PROSES SELESAI! 🎉")
        print(f"📋 Silakan cek file: {OUTPUT_SHEET_URL}")
        print(f"   • Sheet 'Sisa' berhasil diperbarui")
        print(f"   ⏰ Waktu total: {duration.seconds // 60}m {duration.seconds % 60}s")
        
        return True
        
    except Exception as e:
        error_message = f"""
PROSES SISA KUOTA GAGAL ❌

📅 Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
⚠️ Error: {str(e)}

🔧 Traceback:
{traceback.format_exc()}
"""
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()
        send_email_notification("PROSES DATA GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN SCRIPT
# ============================
if __name__ == "__main__":
    process_step_by_step()
