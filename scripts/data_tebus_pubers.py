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
# LOAD CREDENTIALS & EMAIL
# ============================
creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_json:
    raise ValueError("❌ SECRET GOOGLE_APPLICATION_CREDENTIALS_JSON TIDAK TERBACA")

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")

if not SENDER_EMAIL or not SENDER_EMAIL_PASSWORD or not RECIPIENT_EMAILS:
    raise ValueError("❌ SECRET EMAIL TIDAK LENGKAP")

try:
    recipient_list = json.loads(RECIPIENT_EMAILS)
except json.JSONDecodeError:
    recipient_list = [e.strip() for e in RECIPIENT_EMAILS.split(",")]

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
# UTILITIES
# ============================
def clean_nik(nik_value):
    if pd.isna(nik_value) or nik_value is None:
        return None
    cleaned = re.sub(r"\D", "", str(nik_value))
    if len(cleaned) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned}")
    return cleaned if cleaned else None

def parse_tanggal_tebus(val):
    if pd.isna(val) or val in ("", None):
        return None
    if isinstance(val, datetime):
        return val
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val), fmt)
        except ValueError:
            continue
    print(f"⚠️  Format tanggal tidak dikenali: {val}")
    return None

def urutkan_data_per_nik(group):
    g = group.copy()
    g["TGL_SORT"] = g["TGL TEBUS"].apply(parse_tanggal_tebus)
    g = g[g["TGL_SORT"].notna()]
    return g.sort_values("TGL_SORT")

# ============================
# EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_CONFIG["sender_email"]
        msg["To"] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg["Subject"] = subject

        color = "green" if is_success else "red"
        bg = "#f0f8f0" if is_success else "#ffe6e6"

        body = f"""
        <html>
        <body>
        <h2 style="color:{color}">{subject}</h2>
        <div style="background:{bg};padding:15px;border-radius:6px">
        {message.replace(chr(10), "<br>")}
        </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(
                EMAIL_CONFIG["sender_email"],
                EMAIL_CONFIG["sender_password"]
            )
            server.send_message(msg)

        return True
    except Exception as e:
        print(f"❌ Email gagal: {e}")
        return False

# ============================
# DOWNLOAD EXCEL
# ============================
def download_excel_files(folder_id, save_folder=SAVE_FOLDER):
    os.makedirs(save_folder, exist_ok=True)
    q = f"'{folder_id}' in parents and mimeType contains 'spreadsheetml'"
    files = drive_service.files().list(q=q, fields="files(id,name)").execute()["files"]

    if not files:
        raise ValueError("Tidak ada file Excel di folder Drive")

    paths = []
    for f in files:
        req = drive_service.files().get_media(fileId=f["id"])
        path = os.path.join(save_folder, f["name"])
        with io.FileIO(path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        paths.append(path)
    return paths

# ============================
# WRITE GOOGLE SHEET (FIX GRID)
# ============================
def write_to_google_sheet(ws, df):
    print(f"📤 Menulis {len(df)} baris ke Google Sheets")
    ws.clear()

    data = [df.columns.tolist()] + df.values.tolist()
    total_rows = len(data)
    total_cols = len(df.columns)

    # ===== FIX GRID LIMIT (WAJIB) =====
    if ws.row_count < total_rows:
        ws.add_rows(total_rows - ws.row_count)
        print(f"➕ Tambah baris: {total_rows - ws.row_count}")

    if ws.col_count < total_cols:
        ws.add_cols(total_cols - ws.col_count)
        print(f"➕ Tambah kolom: {total_cols - ws.col_count}")
    # =================================

    CHUNK = 10000
    for i in range(0, total_rows, CHUNK):
        chunk = data[i:i + CHUNK]
        start = f"A{i + 1}"
        print(f"   📄 Menulis baris {i+1}-{i+len(chunk)}")
        ws.update(
            range_name=start,
            values=chunk,
            value_input_option="USER_ENTERED"
        )
        time.sleep(2)

# ============================
# MAIN
# ============================
def main():
    try:
        excel_files = download_excel_files(FOLDER_ID)
        all_data = []

        for f in excel_files:
            df = pd.read_excel(f, dtype=str)
            if "NIK" not in df.columns:
                continue
            df["NIK"] = df["NIK"].apply(clean_nik)
            df = df[df["NIK"].notna()]
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
        for nik, grp in combined.groupby("NIK"):
            g = urutkan_data_per_nik(grp)
            texts = []
            for i, r in enumerate(g.itertuples(), 1):
                texts.append(
                    f"{i}) {r.NAMA_PETANI} Tgl {r._13} No {r._2} "
                    f"Kios {r._3} Kec {r._1}"
                )
            nama = g["NAMA PETANI"].iloc[0] if len(g) else ""
            rows.append([nik, nama, "\n".join(texts)])

        out_df = pd.DataFrame(rows, columns=["NIK", "Nama", "Data"])

        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(
                title=SHEET_NAME,
                rows=max(1000, len(out_df) + 10),
                cols=len(out_df.columns)
            )

        write_to_google_sheet(ws, out_df)

        send_email_notification(
            "REKAP DATA BERHASIL",
            f"Total NIK: {len(out_df)}",
            True
        )

        print("🎉 SELESAI TANPA ERROR")
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
