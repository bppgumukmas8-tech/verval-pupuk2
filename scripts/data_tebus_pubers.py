import os
import io
import json
import pandas as pd
import gspread
import re
from gspread_dataframe import set_with_dataframe
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
# PROSES UTAMA
# ============================
try:
    log = []
    all_data = []
    total_rows = 0
    file_count = 0
    nik_cleaning_log = []

    print("🔍 Memulai proses rekap data...")
    print(f"📧 Email pengirim: {SENDER_EMAIL}")
    print(f"📧 Email penerima: {', '.join(recipient_list)}")

    # Download semua Excel
    excel_files = download_excel_files(FOLDER_ID)
    print(f"📁 Berhasil download {len(excel_files)} file Excel")

    for fpath in excel_files:
        file_count += 1
        try:
            df = pd.read_excel(fpath, dtype=str)  # pastikan NIK terbaca full string
        except Exception as e:
            print(f"⚠️  Gagal membaca file {os.path.basename(fpath)}: {str(e)}")
            log.append(f"- {os.path.basename(fpath)}: GAGAL DIBACA - {str(e)}")
            continue

        # PROSES BERSIHKAN NIK
        original_nik_count = len(df)
        
        # Pastikan kolom NIK ada
        if 'NIK' not in df.columns:
            print(f"⚠️  Kolom NIK tidak ditemukan di file {os.path.basename(fpath)}")
            log.append(f"- {os.path.basename(fpath)}: KOLOM NIK TIDAK DITEMUKAN")
            continue
            
        df['NIK_ORIGINAL'] = df['NIK']  # Simpan nilai asli untuk logging
        df['NIK'] = df['NIK'].apply(clean_nik)

        # Log NIK yang dibersihkan
        cleaned_niks = df[df['NIK_ORIGINAL'] != df['NIK']][['NIK_ORIGINAL', 'NIK']]
        for _, row in cleaned_niks.iterrows():
            nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")

        # Hapus baris dengan NIK kosong setelah cleaning
        df = df[df['NIK'].notna()]
        cleaned_nik_count = len(df)

        total_rows += cleaned_nik_count
        log.append(f"- {os.path.basename(fpath)}: {original_nik_count} -> {cleaned_nik_count} baris (setelah cleaning NIK)")
        all_data.append(df)

    # Gabungkan semua data
    combined = pd.concat(all_data, ignore_index=True)

    # Pastikan kolom sesuai header
    cols = [
        "KECAMATAN", "NO TRANSAKSI", "NAMA KIOS", "NIK", "NAMA PETANI",
        "UREA", "NPK", "SP36", "ZA", "NPK FORMULA", "ORGANIK", "ORGANIK CAIR",
        "TGL TEBUS", "STATUS"
    ]

    # Handle jika ada kolom yang missing
    for col in cols:
        if col not in combined.columns:
            combined[col] = ""

    combined = combined[cols]

    # Rekap per NIK dengan urutan bulan dan tanggal
    output_rows = []
    
    for nik, group in combined.groupby("NIK"):
        # Urutkan data dalam group berdasarkan bulan dan tanggal
        group_sorted = urutkan_data_per_nik(group)
        
        list_info = []
        for i, (_, row) in enumerate(group_sorted.iterrows(), start=1):
            # Format tanggal asli (dd-mm-yyyy)
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
        
        # Ambil nama dari record pertama (asumsi nama sama untuk NIK yang sama)
        nama_petani = group_sorted['NAMA PETANI'].iloc[0] if len(group_sorted) > 0 else ""
        
        output_rows.append([nik, nama_petani, "\n".join(list_info)])

    out_df = pd.DataFrame(output_rows, columns=["NIK", "Nama", "Data"])

    # Tulis ke Google Sheet
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except:
        ws = sh.add_worksheet(SHEET_NAME, rows=1, cols=3)

    ws.clear()
    set_with_dataframe(ws, out_df)

    # Buat summary untuk email
    now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    success_message = f"""
REKAP DATA BERHASIL ✓

📅 Tanggal Proses: {now}
📁 File Diproses: {file_count}
📊 Total Data: {total_rows} baris
👥 Unique NIK: {len(out_df)}
🔧 NIK Dibersihkan: {len(nik_cleaning_log)} entri

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN:
{chr(10).join(nik_cleaning_log[:10])}  # Tampilkan 10 pertama saja
{"... (masih ada yang lain)" if len(nik_cleaning_log) > 10 else ""}

✅ Data telah berhasil diupload ke Google Sheets:
📊 Spreadsheet: {SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}

📝 CATATAN:
Data telah diurutkan berdasarkan bulan (Jan-Des) dan tanggal (termuda-terlama)
untuk setiap NIK.

📍 REPOSITORY: verval-pupuk2/scripts/data_tebus_pubers.py
"""

    # Print ke console
    print(f"✅ Rekap selesai: {now}, File: {file_count}, Baris: {total_rows}, Unique NIK: {len(out_df)}")

    # Kirim email notifikasi sukses
    send_email_notification("REKAP DATA BERHASIL", success_message, is_success=True)

except Exception as e:
    # Buat error message
    error_message = f"""
REKAP DATA GAGAL ❌

📅 Tanggal Proses: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📍 Repository: verval-pupuk2/scripts/data_tebus_pubers.py
⚠️ Error: {str(e)}

🔧 Traceback:
{traceback.format_exc()}
"""
    print("❌ REKAP GAGAL")
    print(error_message)

    # Kirim email notifikasi error
    send_email_notification("REKAP DATA GAGAL", error_message, is_success=False)
