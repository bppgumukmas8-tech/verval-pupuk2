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
FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"
SAVE_FOLDER = "data_bulanan"
SPREADSHEET_ID = "1wcfplBgnpZmYZR-I6p774DZKBjz8cG326F8Z_EK4KDM"
SHEET_NAME = "Rekap_Gabungan"

# ============================
# LOAD CREDENTIALS DAN KONFIGURASI EMAIL
# ============================
creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_json:
    raise ValueError("❌ SECRET GOOGLE_APPLICATION_CREDENTIALS_JSON TIDAK TERBACA")

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
    if pd.isna(nik_value) or nik_value is None:
        return None
    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)
    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik}")
    return cleaned_nik if cleaned_nik else None

# ============================
# FUNGSI KONVERSI TANGGAL
# ============================
def parse_tanggal_tebus(tanggal_str):
    if pd.isna(tanggal_str) or tanggal_str is None or tanggal_str == "":
        return None

    if isinstance(tanggal_str, datetime):
        return tanggal_str

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
# FUNGSI URUTKAN DATA
# ============================
def urutkan_data_per_nik(group):
    group = group.copy()
    group['TGL_TEBS_DATETIME'] = group['TGL TEBUS'].apply(parse_tanggal_tebus)
    group = group[group['TGL_TEBS_DATETIME'].notna()]
    if len(group) == 0:
        return group
    return group.sort_values('TGL_TEBS_DATETIME')

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = subject

        if is_success:
            email_body = f"""
            <html><body>
            <h2 style="color:green;">{subject}</h2>
            <div>{message.replace(chr(10), '<br>')}</div>
            </body></html>
            """
        else:
            email_body = f"""
            <html><body>
            <h2 style="color:red;">{subject}</h2>
            <div>{message.replace(chr(10), '<br>')}</div>
            </body></html>
            """

        msg.attach(MIMEText(email_body, 'html'))

        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(
                EMAIL_CONFIG["sender_email"],
                EMAIL_CONFIG["sender_password"]
            )
            server.send_message(msg)

        return True
    except Exception as e:
        print(f"❌ Gagal mengirim email: {e}")
        return False

# ============================
# DOWNLOAD FILE EXCEL
# ============================
def download_excel_files(folder_id, save_folder=SAVE_FOLDER):
    os.makedirs(save_folder, exist_ok=True)
    query = f"'{folder_id}' in parents and mimeType contains 'spreadsheetml'"
    results = drive_service.files().list(
        q=query,
        fields="files(id,name)"
    ).execute()

    files = results.get("files", [])
    if not files:
        raise ValueError("Tidak ada file Excel")

    paths = []
    for f in files:
        request = drive_service.files().get_media(fileId=f["id"])
        path = os.path.join(save_folder, f["name"])
        with io.FileIO(path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        paths.append(path)
    return paths

# ============================
# TULIS KE GOOGLE SHEET
# ============================
def write_to_google_sheet(worksheet, dataframe):
    print(f"📤 Menulis {len(dataframe)} baris data ke Google Sheets...")

    print("🧹 Membersihkan data lama di sheet...")
    worksheet.clear()

    data_to_update = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
    total_rows_to_write = len(data_to_update)

    # >>> INJECT GRID FIX START <<<
    required_rows = total_rows_to_write
    required_cols = len(dataframe.columns)

    current_rows = worksheet.row_count
    current_cols = worksheet.col_count

    if current_rows < required_rows:
        worksheet.add_rows(required_rows - current_rows)
        print(f"➕ Menambah {required_rows - current_rows} baris sheet")

    if current_cols < required_cols:
        worksheet.add_cols(required_cols - current_cols)
        print(f"➕ Menambah {required_cols - current_cols} kolom sheet")
    # >>> INJECT GRID FIX END <<<

    CHUNK_SIZE = 10000
    chunk_count = (total_rows_to_write + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_index in range(chunk_count):
        start_row = chunk_index * CHUNK_SIZE
        end_row = min(start_row + CHUNK_SIZE, total_rows_to_write)
        current_chunk = data_to_update[start_row:end_row]
        start_cell = f'A{start_row + 1}'

        try:
            worksheet.update(
                range_name=start_cell,
                values=current_chunk,
                value_input_option='USER_ENTERED'
            )
            if chunk_index < chunk_count - 1:
                time.sleep(2)
        except Exception as e:
            time.sleep(5)
            worksheet.update(
                range_name=start_cell,
                values=current_chunk,
                value_input_option='USER_ENTERED'
            )

# ============================
# PROSES UTAMA
# ============================
def main():
    try:
        all_data = []
        excel_files = download_excel_files(FOLDER_ID)

        for fpath in excel_files:
            df = pd.read_excel(fpath, dtype=str)
            if 'NIK' not in df.columns:
                continue
            df['NIK'] = df['NIK'].apply(clean_nik)
            df = df[df['NIK'].notna()]
            all_data.append(df)

        combined = pd.concat(all_data, ignore_index=True)

        cols = [
            "KECAMATAN","NO TRANSAKSI","NAMA KIOS","NIK","NAMA PETANI",
            "UREA","NPK","SP36","ZA","NPK FORMULA","ORGANIK","ORGANIK CAIR",
            "TGL TEBUS","STATUS"
        ]
        for c in cols:
            if c not in combined.columns:
                combined[c] = ""

        combined = combined[cols]

        rows = []
        for nik, group in combined.groupby("NIK"):
            group_sorted = urutkan_data_per_nik(group)
            list_info = []
            for i, (_, row) in enumerate(group_sorted.iterrows(), start=1):
                list_info.append(
                    f"{i}) {row['NAMA PETANI']} Tgl {row['TGL TEBUS']} "
                    f"No {row['NO TRANSAKSI']} Kios {row['NAMA KIOS']}"
                )
            nama = group_sorted['NAMA PETANI'].iloc[0] if len(group_sorted) else ""
            rows.append([nik, nama, "\n".join(list_info)])

        out_df = pd.DataFrame(rows, columns=["NIK", "Nama", "Data"])

        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(
                title=SHEET_NAME,
                rows=max(1000, len(out_df) + 100),
                cols=len(out_df.columns)
            )

        write_to_google_sheet(ws, out_df)

        send_email_notification(
            "REKAP DATA BERHASIL",
            f"Total NIK: {len(out_df)}",
            True
        )
        return True

    except Exception as e:
        send_email_notification(
            "REKAP DATA GAGAL",
            f"{e}\n\n{traceback.format_exc()}",
            False
        )
        raise

if __name__ == "__main__":
    main()
