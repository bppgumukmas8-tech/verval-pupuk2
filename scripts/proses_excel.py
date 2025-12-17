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
from pathlib import Path
import traceback

# ----------------------------------------------------
# KONFIGURASI DARI SECRETS/ENVIRONMENT VARIABLES
# ----------------------------------------------------

# Load configuration from secrets.json or environment variables
def load_config():
    config = {}
    
    # Try to load from secrets.json (for local development)
    secrets_path = Path(__file__).parent / "secrets.json"
    if secrets_path.exists():
        with open(secrets_path, 'r') as f:
            secrets = json.load(f)
            config.update(secrets)
    
    # Override with environment variables (for GitHub Actions)
    config.update({
        'FOLDER_ID': os.environ.get('FOLDER_ID', '1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn'),
        'ARCHIVE_FOLDER_ID': os.environ.get('ARCHIVE_FOLDER_ID', '1ZawIfza3gLheAfl2D5ocliV0LWpzFFD_'),
        'SERVICE_ACCOUNT_FILE': os.environ.get('SERVICE_ACCOUNT_FILE', 'service_account.json'),
        'SENDER_EMAIL': os.environ.get('SENDER_EMAIL'),
        'SENDER_EMAIL_PASSWORD': os.environ.get('SENDER_EMAIL_PASSWORD'),
        'RECIPIENT_EMAILS': os.environ.get('RECIPIENT_EMAILS', ''),
    })
    
    return config

config = load_config()

FOLDER_ID = config['FOLDER_ID']
ARCHIVE_FOLDER_ID = config['ARCHIVE_FOLDER_ID']
SERVICE_ACCOUNT_FILE = config['SERVICE_ACCOUNT_FILE']
SENDER_EMAIL = config['SENDER_EMAIL']
SENDER_EMAIL_PASSWORD = config['SENDER_EMAIL_PASSWORD']
RECIPIENT_EMAILS = [email.strip() for email in config['RECIPIENT_EMAILS'].split(',') if email.strip()]

# ----------------------------------------------------
# FUNGSI NOTIFIKASI EMAIL
# ----------------------------------------------------

def send_email_notification(subject, body, is_success=True):
    """Kirim notifikasi email"""
    if not SENDER_EMAIL or not SENDER_EMAIL_PASSWORD or not RECIPIENT_EMAILS:
        print("⚠ Konfigurasi email tidak lengkap, skip pengiriman email")
        return
    
    try:
        # Setup email
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(RECIPIENT_EMAILS)
        msg['Subject'] = subject
        
        # Email body
        html_body = f"""
        <html>
            <body>
                <div style="font-family: Arial, sans-serif; padding: 20px;">
                    <h2 style="color: {'#2ecc71' if is_success else '#e74c3c'}">
                        {subject}
                    </h2>
                    <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                        <pre style="white-space: pre-wrap; font-family: monospace;">
{body}
                        </pre>
                    </div>
                    <p style="color: #7f8c8d; font-size: 12px;">
                        ⏰ Waktu proses: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                        📁 Repo: verval-pupuk2
                    </p>
                </div>
            </body>
        </html>
        """
        
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email using SMTP
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(SENDER_EMAIL, SENDER_EMAIL_PASSWORD)
            server.send_message(msg)
        
        print("✅ Notifikasi email berhasil dikirim")
        
    except Exception as e:
        print(f"❌ Gagal mengirim email: {str(e)}")

# ----------------------------------------------------
# AUTENTIKASI GOOGLE DRIVE
# ----------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/drive"]

def initialize_drive_service():
    """Initialize Google Drive service with service account"""
    try:
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            # Try to get from environment variable
            service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
            if service_account_json:
                # Write to file
                with open(SERVICE_ACCOUNT_FILE, 'w') as f:
                    f.write(service_account_json)
            else:
                raise FileNotFoundError(f"Service account file '{SERVICE_ACCOUNT_FILE}' not found")
        
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        drive_service = build("drive", "v3", credentials=creds)
        print("✅ Google Drive service berhasil diinisialisasi")
        return drive_service
    except Exception as e:
        print(f"❌ Gagal menginisialisasi Google Drive: {str(e)}")
        raise

drive = initialize_drive_service()

# ----------------------------------------------------
# DOWNLOAD FILE DARI DRIVE (KE MEMORY)
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

# ----------------------------------------------------
# UPLOAD & REPLACE FILE (NAMA FILE BARU)
# ----------------------------------------------------

def upload_replace_file(file_id, file_stream, new_filename):
    media_body = MediaIoBaseUpload(
        file_stream,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True
    )

    result = drive.files().update(
        fileId=file_id,
        body={"name": new_filename},
        media_body=media_body
    ).execute()

    return result["id"], result["name"]

# ----------------------------------------------------
# PINDAHKAN FILE KE FOLDER ARSIP
# ----------------------------------------------------

def move_file_to_folder(file_id, target_folder_id):
    file_info = drive.files().get(
        fileId=file_id,
        fields="parents"
    ).execute()

    previous_parents = ",".join(file_info.get("parents", []))

    drive.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=previous_parents,
        fields="id, parents"
    ).execute()

# ----------------------------------------------------
# PINDAHKAN FILE LAMA DENGAN NAMA SAMA
# ----------------------------------------------------

def move_files_with_same_name(folder_id, new_filename, keep_file_id, archive_folder_id):
    query = (
        f"'{folder_id}' in parents and "
        f"name = '{new_filename}' and "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )

    result = drive.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    for f in result.get("files", []):
        if f["id"] != keep_file_id:
            print(f"📦 Arsipkan file lama: {f['name']}")
            move_file_to_folder(f["id"], archive_folder_id)

# ----------------------------------------------------
# PROSES EXCEL
# ----------------------------------------------------

def process_excel(file_id, file_name):
    print(f"▶ Memproses: {file_name}")

    file_stream = download_drive_file(file_id)
    df = pd.read_excel(file_stream, header=None, dtype=str)

    if len(df) <= 2:
        print("⚠ File terlalu pendek, dilewati.")
        return None

    # Hapus baris pertama & terakhir
    df = df.iloc[1:-1].reset_index(drop=True)

    # Jadikan baris pertama sebagai header
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    df.reset_index(drop=True, inplace=True)

    # Cari kolom TGL INPUT
    possible_cols = ["TGL INPUT", "TGL_INPUT", "TGLINPUT", "tgl input"]
    found_col = None

    for col in df.columns:
        if str(col).strip().replace(" ", "").upper() in [
            p.replace(" ", "").upper() for p in possible_cols
        ]:
            found_col = col
            break

    if not found_col:
        print("⚠ Kolom TGL INPUT tidak ditemukan.")
        return None

    df.rename(columns={found_col: "TGL INPUT"}, inplace=True)
    df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")

    latest = df["TGL INPUT"].max()
    if pd.isna(latest):
        print("⚠ TGL INPUT kosong.")
        return None

    latest_str = latest.strftime("%d-%m-%Y %H:%M")
    note_col = f"Update data input realisasi terakhir {latest_str}"
    df[note_col] = ""

    # Nama file berdasarkan bulan
    bulan_map = {
        "January": "Januari", "February": "Februari", "March": "Maret",
        "April": "April", "May": "Mei", "June": "Juni", "July": "Juli",
        "August": "Agustus", "September": "September",
        "October": "Oktober", "November": "November", "December": "Desember"
    }

    nama_bulan = bulan_map[latest.strftime("%B")]
    new_filename = f"{nama_bulan}.xlsx"

    # TULIS ULANG EXCEL → SHEET TETAP "Worksheet"
    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Worksheet")

    output_stream.seek(0)

    new_id, _ = upload_replace_file(file_id, output_stream, new_filename)

    # Arsipkan file lama dengan nama sama
    move_files_with_same_name(
        folder_id=FOLDER_ID,
        new_filename=new_filename,
        keep_file_id=new_id,
        archive_folder_id=ARCHIVE_FOLDER_ID
    )

    print(f"✔ Selesai → {new_filename} | Sheet: Worksheet")
    return {
        'original_name': file_name,
        'new_name': new_filename,
        'latest_update': latest_str,
        'file_id': new_id
    }

# ----------------------------------------------------
# LIST FILE EXCEL DALAM FOLDER
# ----------------------------------------------------

def list_files_in_folder(folder_id):
    query = (
        f"'{folder_id}' in parents and "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )

    result = drive.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    return result.get("files", [])

# ----------------------------------------------------
# MAIN
# ----------------------------------------------------

def main():
    start_time = datetime.now()
    success_count = 0
    failed_count = 0
    processed_files = []
    error_messages = []
    
    try:
        print("🚀 Memulai proses Excel di Google Drive")
        print(f"📁 Folder sumber: {FOLDER_ID}")
        print(f"📦 Folder arsip: {ARCHIVE_FOLDER_ID}")
        print(f"📧 Email pengirim: {SENDER_EMAIL}")
        print(f"📧 Email penerima: {', '.join(RECIPIENT_EMAILS)}")
        print("=" * 50)
        
        files = list_files_in_folder(FOLDER_ID)

        if not files:
            print("📭 Tidak ada file Excel di folder sumber.")
            email_body = "Tidak ada file Excel yang ditemukan di folder sumber."
            send_email_notification("📭 Proses Excel - Tidak Ada File", email_body, True)
            return

        print(f"📄 Ditemukan {len(files)} file Excel")
        
        for f in files:
            try:
                result = process_excel(f["id"], f["name"])
                if result:
                    success_count += 1
                    processed_files.append(result)
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                error_msg = f"❌ Error processing {f['name']}: {str(e)}"
                print(error_msg)
                error_messages.append(error_msg)
        
        # Buat laporan
        duration = datetime.now() - start_time
        report = f"""
📊 LAPORAN PROSES EXCEL
================================
📅 Waktu mulai: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
⏱️ Durasi: {duration.total_seconds():.1f} detik
📁 Total file ditemukan: {len(files)}
✅ Berhasil diproses: {success_count}
❌ Gagal diproses: {failed_count}

📋 File yang diproses:
"""
        for i, file in enumerate(processed_files, 1):
            report += f"  {i}. {file['original_name']} → {file['new_name']} (Update: {file['latest_update']})\n"
        
        if error_messages:
            report += f"\n⚠️ Error yang terjadi:\n"
            for error in error_messages:
                report += f"  • {error}\n"
        
        print(report)
        
        # Kirim email notifikasi
        if processed_files or error_messages:
            subject = f"✅ Proses Excel Berhasil" if success_count > 0 else "❌ Proses Excel Gagal"
            subject += f" ({success_count} berhasil, {failed_count} gagal)"
            send_email_notification(subject, report, success_count > 0)
            
    except Exception as e:
        error_body = f"Error utama: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        print(error_body)
        send_email_notification("🚨 Proses Excel Error", error_body, False)

if __name__ == "__main__":
    main()
