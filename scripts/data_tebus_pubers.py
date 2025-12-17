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

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"  # Folder Google Drive
SAVE_FOLDER = "data_bulanan"  # Folder lokal di runner
SPREADSHEET_ID = "1mObLomLJjyz1cM8KagHzMcjqlO6zGW3Rf8Qnwng5SSY"
SHEET_NAME = "Rekap_Gabungan"

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

# Parse recipient emails (bisa berupa string dengan koma dipisah atau list JSON)
try:
    # Coba parse sebagai JSON array
    recipient_list = json.loads(RECIPIENT_EMAILS)
except json.JSONDecodeError:
    # Jika bukan JSON, split berdasarkan koma
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

    # Convert ke string dan hilangkan semua karakter non-digit
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)  # \D = non-digit

    # Validasi panjang NIK (biasanya 16 digit)
    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")

    return cleaned_nik if cleaned_nik else None

# ============================
# FUNGSI KONVERSI TANGGAL
# ============================
def parse_tanggal_tebus(tanggal_str):
    """
    Mengonversi string tanggal format dd-mm-yyyy menjadi datetime object
    """
    if pd.isna(tanggal_str) or tanggal_str is None or tanggal_str == "":
        return None
    
    try:
        # Coba parsing format dd-mm-yyyy
        return datetime.strptime(str(tanggal_str), '%d-%m-%Y')
    except ValueError:
        try:
            # Coba format lain jika ada
            return datetime.strptime(str(tanggal_str), '%d/%m/%Y')
        except ValueError:
            try:
                # Coba format yyyy-mm-dd
                return datetime.strptime(str(tanggal_str), '%Y-%m-%d')
            except ValueError:
                print(f"⚠️  Format tanggal tidak dikenali: {tanggal_str}")
                return None

# ============================
# FUNGSI URUTKAN DATA BERDASARKAN BULAN DAN TANGGAL
# ============================
def urutkan_data_per_nik(group):
    """
    Mengurutkan data dalam group NIK berdasarkan bulan (Jan-Des) dan tanggal
    """
    # Tambahkan kolom bulan dan datetime untuk sorting
    group = group.copy()
    group['TGL_TEBS_DATETIME'] = group['TGL TEBUS'].apply(parse_tanggal_tebus)
    
    # Hapus data dengan tanggal tidak valid
    group = group[group['TGL_TEBS_DATETIME'].notna()]
    
    if len(group) == 0:
        return group
    
    # Urutkan berdasarkan datetime
    group = group.sort_values('TGL_TEBS_DATETIME')
    
    return group

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """
    Mengirim notifikasi email tentang status proses
    """
    try:
        # Konfigurasi email
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
# FUNGSI UNTUK MENULIS DATA KE GOOGLE SHEETS (DIPERBAIKI)
# ============================
def write_to_google_sheet(worksheet, dataframe):
    """
    Menulis DataFrame ke Google Sheets dengan metode chunking untuk menghindari error API
    """
    try:
        print(f"📤 Menulis {len(dataframe)} baris data ke Google Sheets...")
        
        # 1. Clear worksheet terlebih dahulu
        print("🧹 Membersihkan data lama di sheet...")
        worksheet.clear()
        
        # 2. Konversi DataFrame menjadi list of lists (format API)
        data_to_update = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
        total_rows_to_write = len(data_to_update)
        
        print(f"📦 Ukuran data: {total_rows_to_write} baris x {len(dataframe.columns)} kolom")
        
        # 3. Tentukan ukuran chunk yang aman (10,000 baris per request)
        CHUNK_SIZE = 10000
        chunk_count = (total_rows_to_write + CHUNK_SIZE - 1) // CHUNK_SIZE  # Pembulatan ke atas
        
        print(f"🔀 Membagi data menjadi {chunk_count} chunk...")
        
        # 4. Tulis data per chunk dengan jeda antar chunk
        for chunk_index in range(chunk_count):
            start_row = chunk_index * CHUNK_SIZE
            end_row = min(start_row + CHUNK_SIZE, total_rows_to_write)
            
            # Ambil potongan data untuk chunk ini
            current_chunk = data_to_update[start_row:end_row]
            
            # Tentukan sel awal untuk chunk ini (A1, A10001, A20001, dst.)
            start_cell = f'A{start_row + 1}'
            
            print(f"   📄 Menulis chunk {chunk_index + 1}/{chunk_count}: baris {start_row + 1}-{end_row}...")
            
            try:
                # Gunakan update sederhana dengan value_input_option
                worksheet.update(start_cell, current_chunk, value_input_option='USER_ENTERED')
                
                # Tambahkan jeda singkat ANTAR CHUNK untuk menghindari beban API berlebihan
                if chunk_index < chunk_count - 1:  # Jangan tunggu di chunk terakhir
                    time.sleep(2)  # Jeda 2 detik antara chunk
                    
            except Exception as chunk_error:
                print(f"❌ Error pada chunk {chunk_index + 1}: {str(chunk_error)}")
                print("🔄 Mencoba lagi dengan jeda yang lebih lama...")
                
                # Coba sekali lagi dengan jeda lebih lama
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
# PROSES UTAMA
# ============================
def main():
    try:
        log = []
        all_data = []
        total_rows = 0
        file_count = 0
        nik_cleaning_log = []

        print("=" * 60)
        print("🔍 MEMULAI PROSES REKAP DATA")
        print("=" * 60)
        print(f"📧 Email pengirim: {SENDER_EMAIL}")
        print(f"📧 Email penerima: {', '.join(recipient_list)}")
        print()

        # 1. Download semua Excel
        excel_files = download_excel_files(FOLDER_ID)
        print(f"📁 Berhasil download {len(excel_files)} file Excel")
        print()

        # 2. Proses setiap file
        for fpath in excel_files:
            file_count += 1
            filename = os.path.basename(fpath)
            print(f"🔄 Memproses file {file_count}/{len(excel_files)}: {filename}")
            
            try:
                df = pd.read_excel(fpath, dtype=str)
            except Exception as e:
                print(f"   ❌ Gagal membaca file: {str(e)}")
                log.append(f"- {filename}: GAGAL DIBACA - {str(e)}")
                continue

            # Cek kolom NIK
            if 'NIK' not in df.columns:
                print(f"   ⚠️  Kolom NIK tidak ditemukan")
                log.append(f"- {filename}: KOLOM NIK TIDAK DITEMUKAN")
                continue
                
            # Simpan original dan bersihkan NIK
            original_nik_count = len(df)
            df['NIK_ORIGINAL'] = df['NIK']
            df['NIK'] = df['NIK'].apply(clean_nik)

            # Log NIK yang dibersihkan
            cleaned_niks = df[df['NIK_ORIGINAL'] != df['NIK']][['NIK_ORIGINAL', 'NIK']]
            for _, row in cleaned_niks.iterrows():
                nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")

            # Hapus baris dengan NIK kosong
            df = df[df['NIK'].notna()]
            cleaned_nik_count = len(df)

            total_rows += cleaned_nik_count
            log.append(f"- {filename}: {original_nik_count} -> {cleaned_nik_count} baris")
            all_data.append(df)
            
            print(f"   ✅ Berhasil: {original_nik_count} → {cleaned_nik_count} baris")

        print()
        
        if not all_data:
            raise ValueError("❌ Tidak ada data yang berhasil diproses dari semua file")

        # 3. Gabungkan semua data
        print("🔄 Menggabungkan data dari semua file...")
        combined = pd.concat(all_data, ignore_index=True)
        print(f"✅ Total data gabungan: {len(combined)} baris")

        # 4. Pastikan kolom sesuai
        cols = [
            "KECAMATAN", "NO TRANSAKSI", "NAMA KIOS", "NIK", "NAMA PETANI",
            "UREA", "NPK", "SP36", "ZA", "NPK FORMULA", "ORGANIK", "ORGANIK CAIR",
            "TGL TEBUS", "STATUS"
        ]

        for col in cols:
            if col not in combined.columns:
                combined[col] = ""

        combined = combined[cols]

        # 5. Rekap per NIK dengan urutan bulan dan tanggal
        print("🔄 Membuat rekap per NIK...")
        output_rows = []
        
        unique_nik_count = 0
        for nik, group in combined.groupby("NIK"):
            unique_nik_count += 1
            
            # Urutkan data dalam group
            group_sorted = urutkan_data_per_nik(group)
            
            list_info = []
            for i, (_, row) in enumerate(group_sorted.iterrows(), start=1):
                tgl_tebus = row['TGL TEBUS']
                
                text = (
                    f"{i}) {row['NAMA PETANI']} Tgl Tebus {tgl_tebus} "
                    f"No Transaksi {row['NO TRANSAKSI']} Kios {row['NAMA KIOS']}, Kecamatan {row['KECAMATAN']}, "
                    f"Urea {row['UREA']} kg, NPK {row['NPK']} kg, SP36 {row['SP36']} kg, "
                    f"ZA {row['ZA']} kg, NPK Formula {row['NPK FORMULA']} kg, "
                    f"Organik {row['ORGANIK']} kg, Organik Cair {row['ORGANIK CAIR']} kg, "
                    f"Status {row['STATUS']}"
                )
                list_info.append(text)
            
            nama_petani = group_sorted['NAMA PETANI'].iloc[0] if len(group_sorted) > 0 else ""
            output_rows.append([nik, nama_petani, "\n".join(list_info)])

        out_df = pd.DataFrame(output_rows, columns=["NIK", "Nama", "Data"])
        print(f"✅ Rekap selesai: {unique_nik_count} NIK unik ditemukan")

        # 6. Tulis ke Google Sheet (DENGAN FUNGSI BARU)
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
                rows=max(1000, len(out_df) + 100), 
                cols=len(out_df.columns)
            )
            print(f"✅ Sheet '{SHEET_NAME}' berhasil dibuat")
        
        # Tulis data dengan fungsi yang sudah diperbaiki
        write_to_google_sheet(ws, out_df)

        # 7. Buat laporan sukses
        print()
        print("=" * 60)
        print("✅ PROSES SELESAI")
        print("=" * 60)
        
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
REKAP DATA BERHASIL DIPERBAIKI ✓

📅 Tanggal Proses: {now}
📁 File Diproses: {file_count}
📊 Total Data Awal: {total_rows} baris
👥 Unique NIK: {len(out_df)}
🔧 NIK Dibersihkan: {len(nik_cleaning_log)} entri

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN (10 pertama):
{chr(10).join(nik_cleaning_log[:10])}
{"... (masih ada " + str(len(nik_cleaning_log) - 10) + " entri lainnya)" if len(nik_cleaning_log) > 10 else ""}

✅ DATA TELAH BERHASIL DIUPLOAD:
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}
📈 Baris Data: {len(out_df)}

🔧 PERBAIKAN YANG DITERAPKAN:
1. Mengganti set_with_dataframe() dengan metode chunking
2. Menambahkan retry mechanism untuk error sementara
3. Jeda antar request untuk menghindari rate limiting

📍 REPOSITORY: verval-pupuk2/scripts/data_tebus_pubers.py
"""

        print(f"📊 Ringkasan: {now}, File: {file_count}, Data: {total_rows}, NIK: {len(out_df)}")

        # 8. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        send_email_notification("REKAP DATA BERHASIL (DIPERBAIKI)", success_message, is_success=True)
        
        print("\n" + "=" * 60)
        print("🎉 SEMUA PROSES TELAH BERHASIL!")
        print("=" * 60)
        
        return True

    except Exception as e:
        # Buat error message
        error_message = f"""
REKAP DATA GAGAL ❌

📅 Tanggal Proses: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📍 Repository: verval-pupuk2/scripts/data_tebus_pubers.py
📊 Status: Gagal saat menulis ke Google Sheets

⚠️ ERROR DETAILS:
{str(e)}

🔧 TROUBLESHOOTING:
1. Pastikan service account punya akses EDITOR di Google Sheet
2. Cek apakah spreadsheet ID benar: {SPREADSHEET_ID}
3. Service Account: github-verval-pupuk@verval-pupuk-automation.iam.gserviceaccount.com

🔧 TRACEBACK:
{traceback.format_exc()[:500]}... (truncated)
"""
        print("\n" + "=" * 60)
        print("❌ REKAP GAGAL")
        print("=" * 60)
        print(error_message)

        # Kirim email notifikasi error
        send_email_notification("REKAP DATA GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN PROSES UTAMA
# ============================
if __name__ == "__main__":
    main()
