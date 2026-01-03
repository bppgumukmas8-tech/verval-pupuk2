import os
import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from collections import defaultdict

# ----------------------------------------------------
# KONFIGURASI (TETAP)
# ----------------------------------------------------

FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"
ARCHIVE_FOLDER_ID = "1ZawIfza3gLheAfl2D5ocliV0LWpzFFD_"

SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

RECIPIENT_LIST = [e.strip() for e in RECIPIENT_EMAILS.split(",") if e.strip()]

# ----------------------------------------------------
# LOGGING (TETAP)
# ----------------------------------------------------

log_messages = []
processed_files = []
error_messages = []

def add_log(message, is_error=False):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    log_messages.append(log_entry)
    if is_error:
        error_messages.append(log_entry)
    print(log_entry)

# ----------------------------------------------------
# AUTENTIKASI GOOGLE DRIVE (TETAP)
# ----------------------------------------------------

def initialize_drive():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    if SERVICE_ACCOUNT_JSON:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(SERVICE_ACCOUNT_JSON), scopes=SCOPES
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            "service_account.json", scopes=SCOPES
        )
    return build("drive", "v3", credentials=creds)

drive = initialize_drive()

# ----------------------------------------------------
# DRIVE UTIL (TETAP)
# ----------------------------------------------------

def download_drive_file(file_id):
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def move_file_to_folder(file_id, target_folder_id):
    parents = drive.files().get(fileId=file_id, fields="parents").execute().get("parents", [])
    drive.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=",".join(parents),
        fields="id, parents"
    ).execute()

def list_files_in_folder(folder_id):
    result = drive.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id, name)"
    ).execute()
    return result.get("files", [])

# ----------------------------------------------------
# FUNGSI BANTUAN UNTUK PARSING TANGGAL
# ----------------------------------------------------

def parse_date_safe(date_str):
    """Parse tanggal dengan format yang lebih fleksibel"""
    if pd.isna(date_str):
        return None
    
    # Coba berbagai format
    date_formats = [
        '%d-%m-%Y',  # 3-1-2026
        '%d/%m/%Y',  # 3/1/2026
        '%Y-%m-%d',  # 2026-01-03
        '%Y/%m/%d',  # 2026/01/03
        '%d %b %Y',  # 3 Jan 2026
        '%d %B %Y',  # 3 Januari 2026
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    
    # Jika tidak ada yang cocok, coba parsing dengan pandas
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except:
        return None

def extract_month_from_date(date_value):
    """Ekstrak bulan dari tanggal dengan handling error"""
    if pd.isna(date_value):
        return None
    
    bulan_map = {
        1: "Januari", 2: "Februari", 3: "Maret",
        4: "April", 5: "Mei", 6: "Juni",
        7: "Juli", 8: "Agustus", 9: "September",
        10: "Oktober", 11: "November", 12: "Desember"
    }
    
    try:
        if isinstance(date_value, datetime):
            month_num = date_value.month
        else:
            # Jika bukan datetime, coba konversi
            parsed = parse_date_safe(str(date_value))
            if parsed:
                month_num = parsed.month
            else:
                return None
        
        return bulan_map.get(month_num, None)
    except:
        return None

# ----------------------------------------------------
# PROSES EXCEL → RETURN DATAFRAME & BULAN (MODIFIKASI)
# ----------------------------------------------------

def process_excel(file_id, file_name):
    add_log(f"▶ Membaca: {file_name}")

    df = pd.read_excel(download_drive_file(file_id), header=None, dtype=str)

    if len(df) <= 2:
        add_log("⚠ File terlalu pendek", is_error=True)
        return None

    df = df.iloc[1:-1].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # Cari kolom TGL INPUT dan TGL TEBUS
    tgl_input_col = None
    tgl_tebus_col = None
    
    for col in df.columns:
        col_clean = str(col).replace(" ", "").upper()
        if col_clean == "TGLINPUT":
            tgl_input_col = col
        elif col_clean == "TGLTEBUS":
            tgl_tebus_col = col

    if not tgl_input_col:
        add_log("⚠ Kolom TGL INPUT tidak ditemukan", is_error=True)
        return None
    
    if not tgl_tebus_col:
        add_log("⚠ Kolom TGL TEBUS tidak ditemukan", is_error=True)
        return None

    # Rename kolom untuk konsistensi
    df.rename(columns={tgl_input_col: "TGL INPUT", tgl_tebus_col: "TGL TEBUS"}, inplace=True)
    
    # Konversi tanggal dengan cara yang lebih aman
    df["TGL INPUT"] = df["TGL INPUT"].apply(parse_date_safe)
    df["TGL TEBUS"] = df["TGL TEBUS"].apply(parse_date_safe)

    # Cari bulan untuk TGL TEBUS
    bulan_tebus_list = []
    for date_val in df["TGL TEBUS"]:
        bulan = extract_month_from_date(date_val)
        if bulan:
            bulan_tebus_list.append(bulan)
    
    if not bulan_tebus_list:
        add_log("⚠ Tidak ada bulan yang valid di TGL TEBUS", is_error=True)
        return None
    
    # Tentukan bulan berdasarkan modus (yang paling sering muncul)
    from collections import Counter
    bulan_counter = Counter(bulan_tebus_list)
    bulan_tebus = bulan_counter.most_common(1)[0][0]
    add_log(f"  - Bulan TGL TEBUS: {bulan_tebus} (ditemukan {bulan_counter[bulan_tebus]} kali)")
    
    # Cari tanggal terbaru untuk TGL INPUT (untuk catatan update)
    latest_input = df["TGL INPUT"].max()
    
    if pd.isna(latest_input):
        add_log("⚠ TGL INPUT tidak valid", is_error=True)
        return None

    # Bulan dari TGL INPUT (untuk logging saja)
    bulan_input = extract_month_from_date(latest_input)

    return {
        "bulan_input": bulan_input,  # Hanya untuk logging
        "bulan_tebus": bulan_tebus,  # Untuk nama file (berdasarkan modus TGL TEBUS)
        "latest_input": latest_input,  # Tanggal update terakhir
        "df": df,
        "source_file_id": file_id,
        "source_name": file_name,
        "month_counts": dict(bulan_counter)  # Untuk debugging
    }

# ----------------------------------------------------
# MAIN
# ----------------------------------------------------

def main():
    files = list_files_in_folder(FOLDER_ID)
    if not files:
        add_log("Tidak ada file Excel.")
        return

    monthly_data = defaultdict(list)  # Group berdasarkan bulan TGL TEBUS
    monthly_sources = defaultdict(list)

    # 1️⃣ BACA SEMUA FILE
    for f in files:
        result = process_excel(f["id"], f["name"])
        if result:
            # Group berdasarkan bulan TGL TEBUS
            monthly_data[result["bulan_tebus"]].append(result["df"])
            monthly_sources[result["bulan_tebus"]].append(result)
            add_log(f"  - File '{f['name']}' dikelompokkan ke bulan {result['bulan_tebus']}")

    # 2️⃣ GABUNG PER BULAN (berdasarkan TGL TEBUS)
    for bulan_tebus, df_list in monthly_data.items():
        add_log(f"📊 Menggabungkan {len(df_list)} file untuk bulan {bulan_tebus}")

        # Gabungkan semua dataframe untuk bulan ini
        final_df = pd.concat(df_list, ignore_index=True)
        
        # Cari tanggal update terbaru dari TGL INPUT
        latest_input_date = final_df["TGL INPUT"].max()
        
        if pd.isna(latest_input_date):
            add_log("⚠ Tidak ada tanggal valid di TGL INPUT untuk file gabungan", is_error=True)
            continue
        
        # Tambahkan catatan update dengan tanggal dari TGL INPUT
        note_col = f"Update data input realisasi terakhir {latest_input_date.strftime('%d-%m-%Y %H:%M')}"
        final_df[note_col] = ""

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            final_df.to_excel(writer, index=False, sheet_name="Worksheet")
        output.seek(0)

        # Nama file berdasarkan bulan TGL TEBUS
        filename = f"{bulan_tebus}.xlsx"

        existing = drive.files().list(
            q=f"'{FOLDER_ID}' in parents and name='{filename}'",
            fields="files(id)"
        ).execute().get("files", [])

        media = MediaIoBaseUpload(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        if existing:
            drive.files().update(fileId=existing[0]["id"], media_body=media).execute()
            add_log(f"  - File {filename} diperbarui")
        else:
            drive.files().create(
                body={"name": filename, "parents": [FOLDER_ID]},
                media_body=media
            ).execute()
            add_log(f"  - File {filename} dibuat baru")

        # 3️⃣ ARSIPKAN SEMUA FILE SUMBER
        for src in monthly_sources[bulan_tebus]:
            move_file_to_folder(src["source_file_id"], ARCHIVE_FOLDER_ID)
            processed_files.append({
                "original_name": src["source_name"],
                "new_name": filename,
                "bulan_tebus": bulan_tebus,
                "bulan_input": src["bulan_input"]
            })

        add_log(f"✔ {filename} selesai & {len(monthly_sources[bulan_tebus])} file sumber diarsipkan")
        add_log(f"  - Tanggal update terakhir: {latest_input_date.strftime('%d-%m-%Y %H:%M')}")

    # Ringkasan
    add_log(f"\n📋 RINGKASAN:")
    add_log(f"  - Total file diproses: {len(files)}")
    add_log(f"  - File berhasil digabung: {len(monthly_data)} bulan")
    for bulan in monthly_data:
        add_log(f"    • {bulan}: {len(monthly_data[bulan])} file")

# ----------------------------------------------------

if __name__ == "__main__":
    main()
