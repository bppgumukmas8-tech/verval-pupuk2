import os
import io
import json
import pandas as pd
import gspread
import re
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
# FUNGSI KONVERSI TANGGAL
# ============================
def parse_tanggal_tebus(tanggal_str):
    """
    Mengonversi string tanggal format dd-mm-yyyy menjadi datetime object
    """
    if pd.isna(tanggal_str) or tanggal_str is None or tanggal_str == "":
        return None
    
    try:
        return datetime.strptime(str(tanggal_str), '%d-%m-%Y')
    except ValueError:
        try:
            return datetime.strptime(str(tanggal_str), '%d/%m/%Y')
        except ValueError:
            try:
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
    group = group.copy()
    group['TGL_TEBS_DATETIME'] = group['TGL TEBUS'].apply(parse_tanggal_tebus)
    
    group = group[group['TGL_TEBS_DATETIME'].notna()]
    
    if len(group) == 0:
        return group
    
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
def write_to_google_sheet(worksheet, dataframe):
    """
    Menulis DataFrame ke Google Sheets dengan metode yang lebih stabil
    """
    try:
        print(f"📝 Menyiapkan data untuk ditulis ({len(dataframe)} baris, {len(dataframe.columns)} kolom)...")
        
        # Konversi DataFrame ke list of lists
        data_to_update = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
        
        print(f"📦 Ukuran data: {len(data_to_update)} baris x {len(data_to_update[0]) if data_to_update else 0} kolom")
        
        # Tulis data sekaligus dengan chunking untuk data besar
        total_rows = len(data_to_update)
        
        if total_rows <= 1000:
            # Untuk data kecil (<1000 baris), tulis sekaligus
            print("🔄 Menulis data sekaligus...")
            worksheet.update('A1', data_to_update, value_input_option='USER_ENTERED')
            print(f"✅ Data berhasil ditulis ({total_rows} baris)")
        else:
            # Untuk data besar, tulis per 500 baris untuk hindari timeout
            print(f"🔄 Data besar terdeteksi, menulis per 500 baris...")
            chunk_size = 500
            
            for i in range(0, total_rows, chunk_size):
                end_idx = min(i + chunk_size, total_rows)
                chunk = data_to_update[i:end_idx]
                start_cell = f'A{i+1}'
                
                print(f"  📄 Menulis chunk {i//chunk_size + 1}: baris {i+1}-{end_idx}...")
                worksheet.update(start_cell, chunk, value_input_option='USER_ENTERED')
            
            print(f"✅ Semua data berhasil ditulis ({total_rows} baris dalam {((total_rows-1)//chunk_size)+1} chunk)")
        
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

        print("=" * 50)
        print("🔍 MEMULAI PROSES REKAP DATA")
        print("=" * 50)
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

        # 6. Tulis ke Google Sheet (DENGAN PERBAIKAN UTAMA)
        print()
        print("=" * 50)
        print("📤 MENULIS DATA KE GOOGLE SHEETS")
        print("=" * 50)
        
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
        print(f"📊 Menulis {len(out_df)} baris data...")
        write_to_google_sheet(ws, out_df)

        # 7. Buat laporan sukses
        print()
        print("=" * 50)
        print("✅ PROSES SELESAI")
        print("=" * 50)
        
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
1. Mengganti set_with_dataframe() dengan update() yang lebih stabil
2. Menambahkan chunking untuk data besar
3. Penanganan worksheet yang lebih baik

📍 REPOSITORY: verval-pupuk2/scripts/data_tebus_pubers.py
"""

        print(f"📊 Ringkasan: {now}, File: {file_count}, Data: {total_rows}, NIK: {len(out_df)}")

        # 8. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        send_email_notification("REKAP DATA BERHASIL (DIPERBAIKI)", success_message, is_success=True)
        
        print("\n" + "=" * 50)
        print("🎉 SEMUA PROSES TELAH BERHASIL!")
        print("=" * 50)
        
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
        print("\n" + "=" * 50)
        print("❌ REKAP GAGAL")
        print("=" * 50)
        print(error_message)

        # Kirim email notifikasi error
        send_email_notification("REKAP DATA GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN PROSES UTAMA
# ============================
if __name__ == "__main__":
    main()
