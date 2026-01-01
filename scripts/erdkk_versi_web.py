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
def proses_data_gabungan(dataframes_list):
    """
    Memproses dan menggabungkan data berdasarkan NIK dan Poktan
    Mirip dengan logika VBA
    """
    # Dictionary untuk menyimpan data hasil
    data_dict = defaultdict(lambda: {
        'data': None,
        'komoditas_set': set(),
        'indeks': None
    })
    
    hasil_rows = []
    header = None
    
    for df_idx, df in enumerate(dataframes_list):
        # Konversi ke numpy array untuk proses cepat (seperti VBA)
        data_array = df.values
        
        # Set header dari file pertama
        if df_idx == 0:
            header = list(df.columns)
            # Sesuaikan dengan output VBA (kolom komoditas digabung)
            output_header = header[:6] + ["KOMODITAS"] + header[9:] if len(header) > 9 else header
            hasil_rows.append(output_header)
        
        # Proses setiap baris
        for i in range(len(df)):
            row = df.iloc[i]
            
            # Buat key unik seperti di VBA: KTP|Nama|Desa|Poktan|Kios
            # Asumsi kolom: A=KTP, B=Nama, C=Desa, D=Poktan, E=Kios (sesuaikan jika berbeda)
            key_parts = []
            for col in ['KTP', 'NAMA', 'DESA', 'POKTAN', 'KIOS']:
                col_lower = col.lower()
                if col_lower in df.columns.str.lower():
                    col_name = df.columns[df.columns.str.lower() == col_lower][0]
                    key_parts.append(str(row[col_name]) if not pd.isna(row[col_name]) else "")
                else:
                    key_parts.append("")
            
            key = "|".join(key_parts)
            
            if key not in data_dict:
                # Data baru, tambahkan ke hasil
                data_dict[key]['indeks'] = len(hasil_rows)
                
                # Siapkan row hasil
                hasil_row = []
                
                # Kolom 1-5 (A-E): Data utama
                for j in range(5):
                    if j < len(header):
                        val = row[header[j]]
                        # KTP sebagai teks
                        if j == 0:
                            hasil_row.append(str(val) if not pd.isna(val) else "")
                        else:
                            hasil_row.append(val if not pd.isna(val) else "")
                    else:
                        hasil_row.append("")
                
                # Kolom 6: Gabungkan komoditas dari kolom G, H, I (indeks 6,7,8)
                komoditas_list = []
                for col_idx in [6, 7, 8]:
                    if col_idx < len(header):
                        komoditas_list.append(row[header[col_idx]])
                
                komoditas_str = gabung_komoditas_unique(komoditas_list)
                data_dict[key]['komoditas_set'] = set(komoditas_str.split()) if komoditas_str else set()
                hasil_row.append(komoditas_str)
                
                # Kolom 7+: Data numerik (kolom 9+ dalam original)
                for j in range(9, len(header)):
                    if j < len(header):
                        val = row[header[j]]
                        # Konversi ke numeric jika mungkin
                        try:
                            if pd.isna(val) or val == "":
                                num_val = 0
                            else:
                                num_val = float(str(val).replace(',', ''))
                        except:
                            num_val = 0
                        hasil_row.append(num_val)
                    else:
                        hasil_row.append(0)
                
                data_dict[key]['data'] = hasil_row
                hasil_rows.append(hasil_row)
                
            else:
                # Update data yang sudah ada
                idx = data_dict[key]['indeks']
                existing_row = hasil_rows[idx]
                
                # Update komoditas
                komoditas_list = []
                for col_idx in [6, 7, 8]:
                    if col_idx < len(header):
                        komoditas_list.append(row[header[col_idx]])
                
                new_komoditas = gabung_komoditas_unique(komoditas_list)
                if new_komoditas:
                    new_set = set(new_komoditas.split())
                    data_dict[key]['komoditas_set'].update(new_set)
                    existing_row[5] = " ".join(data_dict[key]['komoditas_set'])
                
                # Jumlahkan nilai numerik (kolom 6+)
                for j in range(6, len(existing_row)):
                    if j < len(header) - 3:  # Adjust untuk kolom komoditas
                        val = row[header[j+3]] if j+3 < len(header) else 0
                        try:
                            if pd.isna(val) or val == "":
                                num_val = 0
                            else:
                                num_val = float(str(val).replace(',', ''))
                        except:
                            num_val = 0
                        
                        # Jumlahkan dengan existing
                        try:
                            existing_val = float(existing_row[j]) if existing_row[j] != "" else 0
                        except:
                            existing_val = 0
                        
                        existing_row[j] = existing_val + num_val
    
    return hasil_rows

# ============================
# FUNGSI STANDARDISASI KOLOM
# ============================
def standardize_columns(df):
    """
    Standarisasi nama kolom untuk konsistensi
    """
    # Mapping nama kolom ke standar
    column_mapping = {
        # NIK/KTP
        'nik': 'KTP', 'no ktp': 'KTP', 'ktp': 'KTP', 'no. ktp': 'KTP',
        # Nama
        'nama': 'NAMA', 'nama petani': 'NAMA', 'nama lengkap': 'NAMA',
        # Desa
        'desa': 'DESA', 'kelurahan': 'DESA', 'desa/kel': 'DESA',
        # Poktan
        'poktan': 'POKTAN', 'poktan (kelompok tani)': 'POKTAN', 'kelompok tani': 'POKTAN',
        # Kios
        'kios': 'KIOS', 'nama kios': 'KIOS', 'pengecer': 'KIOS',
        # Komoditas (G, H, I)
        'komoditas': 'KOMODITAS_G', 'jenis tanaman': 'KOMODITAS_G',
        'komoditas1': 'KOMODITAS_G', 'komoditas2': 'KOMODITAS_H', 'komoditas3': 'KOMODITAS_I',
    }
    
    # Rename kolom berdasarkan mapping
    new_columns = []
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if col_lower in column_mapping:
            new_columns.append(column_mapping[col_lower])
        else:
            # Coba cari partial match
            found = False
            for key in column_mapping:
                if key in col_lower:
                    new_columns.append(column_mapping[key])
                    found = True
                    break
            if not found:
                new_columns.append(col)
    
    df.columns = new_columns
    
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
def main():
    try:
        log = []
        all_dataframes = []
        total_rows_original = 0
        total_rows_cleaned = 0
        file_count = 0
        nik_cleaning_log = []

        print("=" * 60)
        print("🔍 MEMULAI PROSES REKAP DATA ERDKK")
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
            except Exception as e:
                print(f"   ❌ Gagal membaca file: {str(e)}")
                log.append(f"- {filename}: GAGAL DIBACA - {str(e)}")
                continue

            # Standarisasi kolom
            df = standardize_columns(df)
            
            # Bersihkan NIK/KTP
            if 'KTP' in df.columns:
                original_count = len(df)
                df['KTP_ORIGINAL'] = df['KTP']
                df['KTP'] = df['KTP'].apply(clean_nik)
                
                # Log perubahan NIK
                cleaned_ktp = df[df['KTP_ORIGINAL'] != df['KTP']][['KTP_ORIGINAL', 'KTP']]
                for _, row in cleaned_ktp.iterrows():
                    nik_cleaning_log.append(f"'{row['KTP_ORIGINAL']}' -> {row['KTP']}")
                
                # Hapus baris dengan NIK kosong
                df = df[df['KTP'].notna()]
                cleaned_count = len(df)
                
                total_rows_original += original_count
                total_rows_cleaned += cleaned_count
                
                log.append(f"- {filename}: {original_count} -> {cleaned_count} baris")
                all_dataframes.append(df)
                
                print(f"   ✅ Berhasil: {original_count} → {cleaned_count} baris")
            else:
                print(f"   ⚠️  Kolom KTP/NIK tidak ditemukan dalam file")
                log.append(f"- {filename}: KOLOM KTP/NIK TIDAK DITEMUKAN")

        print()
        
        if not all_dataframes:
            raise ValueError("❌ Tidak ada data yang berhasil diproses dari semua file")

        # 3. Proses dan gabungkan data (mirip VBA)
        print("🔄 Menggabungkan data berdasarkan NIK dan Poktan (mirip VBA)...")
        hasil_gabungan = proses_data_gabungan(all_dataframes)
        
        print(f"✅ Data berhasil digabung: {len(hasil_gabungan) - 1} baris hasil (termasuk header)")
        print(f"   Header: {len(hasil_gabungan[0])} kolom")

        # 4. Konversi ke DataFrame untuk penulisan
        header = hasil_gabungan[0]
        data = hasil_gabungan[1:]
        df_hasil = pd.DataFrame(data, columns=header)

        # 5. Tulis ke Google Sheet
        print()
        print("=" * 60)
        print("📤 MENULIS DATA KE GOOGLE SHEETS")
        print("=" * 60)
        
        sh = gc.open_by_key(SPREADSHEET_ID)
        
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
        
        # Tulis data
        write_to_google_sheet(ws, hasil_gabungan)

        # 6. Buat laporan sukses
        print()
        print("=" * 60)
        print("✅ PROSES SELESAI")
        print("=" * 60)
        
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
REKAP DATA ERDKK BERHASIL DIPROSES ✓

📅 Tanggal Proses: {now}
📁 Jumlah Kecamatan: {file_count}
📊 Total Data Awal: {total_rows_original} baris
🧹 Data Setelah Cleaning: {total_rows_cleaned} baris
📈 Hasil Gabungan: {len(df_hasil)} baris
🏢 Unique NIK-Poktan: {len(df_hasil)}

📋 DETAIL KECAMATAN:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN (5 pertama):
{chr(10).join(nik_cleaning_log[:5])}
{"... (masih ada " + str(len(nik_cleaning_log) - 5) + " entri lainnya)" if len(nik_cleaning_log) > 5 else ""}

✅ DATA TELAH BERHASIL DIUPLOAD:
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}
📈 Baris Data: {len(df_hasil)}
📊 Kolom Data: {len(df_hasil.columns)}

🔧 FITUR YANG DITERAPKAN (mirip VBA):
1. Penggabungan berdasarkan NIK, Nama, Desa, Poktan, Kios
2. Penggabungan komoditas tanpa duplikat (kolom G,H,I)
3. Penjumlahan nilai numerik untuk data duplikat
4. Format NIK sebagai teks
5. Standarisasi nama kolom

📍 REPOSITORY: proses-erdkk-python
"""

        print(f"📊 Ringkasan: {now}, Kecamatan: {file_count}, Data: {len(df_hasil)} baris")

        # 7. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        send_email_notification("REKAP DATA ERDKK BERHASIL", success_message, is_success=True)
        
        print("\n" + "=" * 60)
        print("🎉 PROSES REKAP DATA ERDKK TELAH BERHASIL!")
        print("=" * 60)
        
        return True

    except Exception as e:
        error_message = f"""
REKAP DATA ERDKK GAGAL ❌

📅 Tanggal Proses: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📍 Folder ID: {FOLDER_ID}
📊 Status: Gagal saat memproses data

⚠️ ERROR DETAILS:
{str(e)}

🔧 TRACEBACK:
{traceback.format_exc()[:500]}... (truncated)
"""
        print("\n" + "=" * 60)
        print("❌ PROSES GAGAL")
        print("=" * 60)
        print(error_message)

        # Kirim email notifikasi error
        send_email_notification("REKAP DATA ERDKK GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN PROSES UTAMA
# ============================
if __name__ == "__main__":
    main()
