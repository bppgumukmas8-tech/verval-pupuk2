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
# FUNGSI GABUNGKAN KOMODITAS
# ============================
def gabung_komoditas_unique(komoditas_list):
    """
    Menggabungkan komoditas dari kolom G, H, I tanpa duplikat
    """
    if not komoditas_list:
        return ""
    
    # Flatten list jika ada list dalam list
    flat_list = []
    for item in komoditas_list:
        if pd.isna(item):
            continue
        if isinstance(item, str):
            # Split jika ada multiple komoditas dalam satu sel
            items = str(item).split()
            flat_list.extend(items)
        else:
            flat_list.append(str(item))
    
    # Hapus duplikat dan kosong
    unique_komoditas = list(set([k for k in flat_list if k.strip()]))
    return " ".join(unique_komoditas)

# ============================
# FUNGSI PROSES DATA (MIRIP DENGAN VBA)
# ============================
def proses_data_gabungan

# ============================
# FUNGSI STANDARDISASI KOLOM
# ============================
def standardize_columns(df):
    """
    Standarisasi nama kolom untuk konsistensi dengan handling yang lebih baik
    """
    if df.empty:
        return df
    
    # Buat mapping lowercase untuk pencarian
    column_mapping = {}
    
    # Mapping komprehensif untuk berbagai variasi nama kolom
    mappings = {
        'KTP': ['nik', 'no ktp', 'ktp', 'no. ktp', 'nomor ktp', 'ktp/nik', 'nik/ktp'],
        'NAMA': ['nama', 'nama petani', 'nama lengkap', 'nama petani', 'nama farmer'],
        'DESA': ['desa', 'kelurahan', 'desa/kel', 'desa/kelurahan', 'alamat'],
        'POKTAN': ['poktan', 'poktan (kelompok tani)', 'kelompok tani', 'nama poktan', 'poktan'],
        'KIOS': ['kios', 'nama kios', 'pengecer', 'nama pengecer', 'kios/pengecer'],
        'KOMODITAS_G': ['komoditas', 'jenis tanaman', 'komoditas1', 'tanaman'],
        'KOMODITAS_H': ['komoditas2', 'jenis tanaman 2'],
        'KOMODITAS_I': ['komoditas3', 'jenis tanaman 3'],
    }
    
    # Create reverse mapping
    for standard_name, variants in mappings.items():
        for variant in variants:
            column_mapping[variant] = standard_name
    
    # Rename columns
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
            for variant, standard in column_mapping.items():
                if variant in col_lower:
                    new_columns.append(standard)
                    found = True
                    break
            if not found:
                new_columns.append(col)
    
    df.columns = new_columns
    
    # Hapus kolom duplikat jika ada
    df = df.loc[:, ~df.columns.duplicated()]
    
    return df

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
# FUNGSI UNTUK MENULIS DATA KE GOOGLE SHEETS
# ============================
def write_to_google_sheet(worksheet, data_rows):
    """
    Menulis data ke Google Sheets dengan metode chunking
    """
    try:
        print(f"📤 Menulis {len(data_rows)} baris data ke Google Sheets...")
        
        # 1. Clear worksheet terlebih dahulu
        print("🧹 Membersihkan data lama di sheet...")
        worksheet.clear()
        
        total_rows_to_write = len(data_rows)
        
        # 2. Tentukan ukuran chunk yang aman
        CHUNK_SIZE = 10000
        chunk_count = (total_rows_to_write + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        print(f"🔀 Membagi data menjadi {chunk_count} chunk...")
        
        # 3. Tulis data per chunk
        for chunk_index in range(chunk_count):
            start_row = chunk_index * CHUNK_SIZE
            end_row = min(start_row + CHUNK_SIZE, total_rows_to_write)
            
            current_chunk = data_rows[start_row:end_row]
            start_cell = f'A{start_row + 1}'
            
            print(f"   📄 Menulis chunk {chunk_index + 1}/{chunk_count}: baris {start_row + 1}-{end_row}...")
            
            try:
                worksheet.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
                
                if chunk_index < chunk_count - 1:
                    time.sleep(2)
                    
            except Exception as chunk_error:
                print(f"❌ Error pada chunk {chunk_index + 1}: {str(chunk_error)}")
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
        print("🔍 MEMULAI PROSES REKAP DATA ERDKK - VERSI WEB")
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
                print(f"   ⚠️  Kolom KTP/NIK tidak ditemukan dalam file")
                log.append(f"- {filename}: KOLOM KTP/NIK TIDAK DITEMUKAN")

        print()
        
        if not all_dataframes:
            raise ValueError("❌ Tidak ada data yang berhasil diproses dari semua file")

        # 3. Proses dan gabungkan data (mirip VBA)
        print(f"🔄 Menggabungkan {len(all_dataframes)} file data...")
        hasil_gabungan = proses_data_gabungan(all_dataframes)
        
        if len(hasil_gabungan) < 2:  # Hanya header, tidak ada data
            raise ValueError("❌ Tidak ada data yang berhasil digabungkan")
        
        print(f"✅ Data berhasil digabung: {len(hasil_gabungan) - 1} baris hasil")
        print(f"   📋 Header: {hasil_gabungan[0]}")

        # 4. Konversi ke DataFrame untuk penulisan
        header = hasil_gabungan[0]
        data = hasil_gabungan[1:]
        df_hasil = pd.DataFrame(data, columns=header)

        # 5. Tulis ke Google Sheet
        print()
        print("=" * 60)
        print("📤 MENULIS DATA KE GOOGLE SHEETS")
        print("=" * 60)
        
        # Buka spreadsheet
        try:
            sh = gc.open_by_key(SPREADSHEET_ID)
            print(f"✅ Spreadsheet ditemukan: {SPREADSHEET_ID}")
        except Exception as e:
            raise ValueError(f"❌ Gagal membuka spreadsheet: {str(e)}")
        
        # Cek atau buat worksheet
        try:
            ws = sh.worksheet(SHEET_NAME)
            print(f"✅ Sheet '{SHEET_NAME}' ditemukan")
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️  Sheet '{SHEET_NAME}' tidak ditemukan, membuat baru...")
            ws = sh.add_worksheet(
                title=SHEET_NAME, 
                rows=max(1000, len(df_hasil) + 100), 
                cols=len(df_hasil.columns)
            )
            print(f"✅ Sheet '{SHEET_NAME}' berhasil dibuat")
        except Exception as e:
            raise ValueError(f"❌ Gagal mengakses worksheet: {str(e)}")
        
        # Tulis data
        success = write_to_google_sheet(ws, hasil_gabungan)
        
        if not success:
            raise ValueError("❌ Gagal menulis data ke Google Sheets")

        # 6. Buat laporan sukses
        print()
        print("=" * 60)
        print("✅ PROSES SELESAI")
        print("=" * 60)
        
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
REKAP DATA ERDKK BERHASIL DIPROSES ✓

📅 Tanggal Proses: {now}
📁 Jumlah File: {file_count}
📊 Total Data Awal: {total_rows_original} baris
🧹 Data Setelah Cleaning: {total_rows_cleaned} baris
📈 Hasil Gabungan: {len(df_hasil)} baris
🏢 Unique NIK-Poktan: {len(df_hasil)}

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN ({min(5, len(nik_cleaning_log))} pertama):
{chr(10).join(nik_cleaning_log[:5])}
{"... (masih ada " + str(len(nik_cleaning_log) - 5) + " entri lainnya)" if len(nik_cleaning_log) > 5 else "Tidak ada NIK yang dibersihkan"}

✅ DATA TELAH BERHASIL DIUPLOAD:
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}
📈 Baris Data: {len(df_hasil)}
📊 Kolom Data: {len(df_hasil.columns)}

🔧 FITUR YANG DITERAPKAN:
1. Penggabungan berdasarkan NIK, Nama, Desa, Poktan, Kios
2. Penggabungan komoditas tanpa duplikat
3. Penjumlahan nilai numerik untuk data duplikat
4. Format NIK sebagai teks
5. Standarisasi nama kolom

📍 REPOSITORY: {os.environ.get('GITHUB_REPOSITORY', 'verval-pupuk2')}
🔄 WORKFLOW RUN: {os.environ.get('GITHUB_RUN_ID', 'N/A')}
"""

        print(f"📊 Ringkasan: {now}, File: {file_count}, Data: {len(df_hasil)} baris")

        # 7. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        email_sent = send_email_notification("REKAP DATA ERDKK BERHASIL", success_message, is_success=True)
        
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
