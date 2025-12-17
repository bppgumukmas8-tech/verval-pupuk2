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

# ----------------------------------------------------
# KONFIGURASI
# ----------------------------------------------------

# FOLDER ID langsung ditulis di kode
FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"           # Folder sumber
ARCHIVE_FOLDER_ID = "1ZawIfza3gLheAfl2D5ocliV0LWpzFFD_"   # Folder arsip

# Baca dari environment variables (mengikuti konvensi repo)
GOOGLE_APPLICATION_CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "")

# Konversi string emails menjadi list
if RECIPIENT_EMAILS:
    RECIPIENT_LIST = [email.strip() for email in RECIPIENT_EMAILS.split(",")]
else:
    RECIPIENT_LIST = []

# ----------------------------------------------------
# INISIALISASI LOGGING
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
# VALIDASI KONFIGURASI
# ----------------------------------------------------

def validate_config():
    """Validasi semua konfigurasi yang diperlukan"""
    required_configs = {
        "GOOGLE_APPLICATION_CREDENTIALS_JSON": GOOGLE_APPLICATION_CREDENTIALS_JSON,
        "SENDER_EMAIL": SENDER_EMAIL,
        "SENDER_EMAIL_PASSWORD": SENDER_PASSWORD,
    }
    
    missing = [name for name, value in required_configs.items() if not value]
    
    if missing:
        error_msg = f"⚠ Konfigurasi berikut belum diatur: {', '.join(missing)}"
        add_log(error_msg, is_error=True)
        raise ValueError(error_msg)
    
    # Validasi tambahan: cek format JSON
    try:
        json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        add_log("✓ Format JSON valid")
    except json.JSONDecodeError as e:
        add_log(f"✗ Format JSON tidak valid: {str(e)}", is_error=True)
        raise
    
    add_log("✓ Semua konfigurasi valid")
    return True

# ----------------------------------------------------
# AUTENTIKASI GOOGLE DRIVE
# ----------------------------------------------------

def authenticate_drive():
    """Autentikasi ke Google Drive menggunakan Service Account"""
    try:
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        
        if not GOOGLE_APPLICATION_CREDENTIALS_JSON:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan di environment variables")
        
        # Parse service account dari string JSON
        service_account_info = json.loads(GOOGLE_APPLICATION_CREDENTIALS_JSON)
        
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, 
            scopes=SCOPES
        )
        
        drive = build("drive", "v3", credentials=creds)
        add_log("✓ Berhasil autentikasi ke Google Drive")
        return drive
    except json.JSONDecodeError as e:
        add_log(f"✗ Format JSON tidak valid: {str(e)}", is_error=True)
        raise
    except Exception as e:
        add_log(f"✗ Gagal autentikasi ke Google Drive: {str(e)}", is_error=True)
        raise

# ----------------------------------------------------
# FUNGSI EMAIL NOTIFICATION
# ----------------------------------------------------

def send_email_notification(subject, body):
    """Mengirim notifikasi email"""
    if not all([SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_LIST]):
        add_log("⚠ Konfigurasi email tidak lengkap, skip pengiriman email")
        return False
    
    try:
        # Setup server SMTP
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        
        # Buat message
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(RECIPIENT_LIST)
        msg["Subject"] = subject
        
        # Tambahkan body
        msg.attach(MIMEText(body, "html"))
        
        # Kirim email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        add_log(f"✓ Email notifikasi terkirim ke {len(RECIPIENT_LIST)} penerima")
        return True
    except Exception as e:
        add_log(f"✗ Gagal mengirim email: {str(e)}", is_error=True)
        return False

# ----------------------------------------------------
# DOWNLOAD FILE DARI DRIVE (KE MEMORY)
# ----------------------------------------------------

def download_drive_file(drive, file_id):
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

def upload_replace_file(drive, file_id, file_stream, new_filename):
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

def move_file_to_folder(drive, file_id, target_folder_id):
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

def move_files_with_same_name(drive, folder_id, new_filename, keep_file_id, archive_folder_id):
    query = (
        f"'{folder_id}' in parents and "
        f"name = '{new_filename}' and "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )

    result = drive.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    moved_files = []
    for f in result.get("files", []):
        if f["id"] != keep_file_id:
            add_log(f"📦 Arsipkan file lama: {f['name']}")
            move_file_to_folder(drive, f["id"], archive_folder_id)
            moved_files.append(f["name"])
    
    return moved_files

# ----------------------------------------------------
# PROSES EXCEL
# ----------------------------------------------------

def process_excel(drive, file_id, file_name):
    add_log(f"▶ Memproses: {file_name}")
    
    try:
        file_stream = download_drive_file(drive, file_id)
        df = pd.read_excel(file_stream, header=None, dtype=str)

        if len(df) <= 2:
            add_log("⚠ File terlalu pendek, dilewati.")
            return None

        # Hapus baris pertama & terakhir
        df = df.iloc[1:-1].reset_index(drop=True)

        # Jadikan baris pertama sebagai header
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header
        df.reset_index(drop=True, inplace=True)

        # Cari kolom TGL INPUT dengan variasi yang lebih banyak
        possible_cols = [
            "TGL INPUT", "TGL_INPUT", "TGLINPUT", "tgl input",
            "TGL.INPUT", "TGL", "TANGGAL INPUT", "TANGGAL_INPUT",
            "Tanggal Input", "tanggal input", "TGL  INPUT"
        ]
        
        found_col = None
        df.columns = df.columns.astype(str)  # Pastikan semua kolom string
        
        for col in df.columns:
            col_clean = str(col).strip().replace(" ", "").replace(".", "").replace("_", "").upper()
            for pattern in possible_cols:
                pattern_clean = pattern.replace(" ", "").replace(".", "").replace("_", "").upper()
                if pattern_clean in col_clean or col_clean in pattern_clean:
                    found_col = col
                    add_log(f"  ✓ Kolom ditemukan: '{col}' -> 'TGL INPUT'")
                    break
            if found_col:
                break

        if not found_col:
            # Debug: tampilkan semua kolom yang ada
            columns_list = ", ".join([f"'{col}'" for col in df.columns[:5]])  # Tampilkan 5 pertama
            if len(df.columns) > 5:
                columns_list += f" ... (total: {len(df.columns)} kolom)"
            add_log(f"⚠ Kolom TGL INPUT tidak ditemukan. Kolom yang ada: {columns_list}")
            return None  # ⬅️ Return None, bukan False

        df.rename(columns={found_col: "TGL INPUT"}, inplace=True)
        
        # Coba parsing tanggal dengan berbagai format
        try:
            df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], errors="coerce")
        except:
            # Fallback: coba format manual
            try:
                df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
            except:
                df["TGL INPUT"] = pd.to_datetime(df["TGL INPUT"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

        latest = df["TGL INPUT"].max()
        if pd.isna(latest):
            add_log(f"⚠ TGL INPUT kosong atau format tidak dikenali.")
            # Coba ambil dari kolom lain yang mungkin berisi tanggal
            date_columns = [col for col in df.columns if any(word in str(col).lower() for word in ['tgl', 'tanggal', 'date', 'time'])]
            if date_columns:
                add_log(f"  ⚠ Coba kolom lain: {date_columns}")
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

        new_id, _ = upload_replace_file(drive, file_id, output_stream, new_filename)

        # Arsipkan file lama dengan nama sama
        moved_files = move_files_with_same_name(
            drive=drive,
            folder_id=FOLDER_ID,
            new_filename=new_filename,
            keep_file_id=new_id,
            archive_folder_id=ARCHIVE_FOLDER_ID
        )

        result_info = {
            "original_name": file_name,
            "new_name": new_filename,
            "file_id": new_id,
            "moved_archives": moved_files,
            "latest_date": latest_str,
            "month": nama_bulan
        }
        
        add_log(f"✔ Selesai → {new_filename} | Sheet: Worksheet | Tanggal: {latest_str}")
        return result_info
        
    except Exception as e:
        add_log(f"✗ Error processing {file_name}: {str(e)}", is_error=True)
        return None

# ----------------------------------------------------
# LIST FILE EXCEL DALAM FOLDER
# ----------------------------------------------------

def list_files_in_folder(drive, folder_id):
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
# BUAT RINGKASAN EMAIL
# ----------------------------------------------------

def create_email_body(processed_files, error_messages):
    """Membuat body email dalam format HTML"""
    current_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    
    # Hitung statistik
    total_warnings = len([msg for msg in error_messages if "⚠" in msg])
    total_errors = len([msg for msg in error_messages if "✗" in msg])
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .container {{ max-width: 1000px; margin: auto; }}
            .header {{ background-color: #4CAF50; color: white; padding: 15px; border-radius: 5px; }}
            .success {{ color: #4CAF50; }}
            .warning {{ color: #FF9800; }}
            .error {{ color: #f44336; }}
            .info {{ color: #2196F3; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .log {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 10px 0; font-family: monospace; }}
            .stats {{ display: flex; justify-content: space-between; margin: 20px 0; }}
            .stat-box {{ background: #f5f5f5; padding: 15px; border-radius: 5px; flex: 1; margin: 0 10px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>📊 Laporan Proses Excel Verval Pupuk</h2>
                <p>Waktu Eksekusi: {current_time}</p>
            </div>
            
            <div class="stats">
                <div class="stat-box">
                    <h3>✅ Berhasil</h3>
                    <p style="font-size: 24px; font-weight: bold;">{len(processed_files)}</p>
                    <p>file</p>
                </div>
                <div class="stat-box">
                    <h3>⚠ Warning</h3>
                    <p style="font-size: 24px; font-weight: bold; color: #FF9800;">{total_warnings}</p>
                    <p>perhatian</p>
                </div>
                <div class="stat-box">
                    <h3>❌ Error</h3>
                    <p style="font-size: 24px; font-weight: bold; color: #f44336;">{total_errors}</p>
                    <p>error sistem</p>
                </div>
            </div>
    """
    
    if processed_files:
        html += """
            <h3>📁 File yang Berhasil Diproses</h3>
            <table>
                <tr>
                    <th>No</th>
                    <th>File Asli</th>
                    <th>File Baru</th>
                    <th>Bulan</th>
                    <th>Tanggal Terakhir</th>
                    <th>Arsip</th>
                </tr>
        """
        
        for i, file_info in enumerate(processed_files, 1):
            archives = ", ".join(file_info['moved_archives']) if file_info['moved_archives'] else "-"
            html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{file_info['original_name']}</td>
                    <td><strong>{file_info['new_name']}</strong></td>
                    <td>{file_info['month']}</td>
                    <td>{file_info['latest_date']}</td>
                    <td>{archives}</td>
                </tr>
            """
        
        html += "</table>"
    else:
        html += "<p><em>Tidak ada file yang berhasil diproses.</em></p>"
    
    if error_messages:
        html += """
            <h3 class="warning">📝 Log Proses</h3>
            <div class="log">
        """
        
        for i, log in enumerate(error_messages, 1):
            html += f"<p>{i}. {log}</p>"
        
        html += "</div>"
    
    # Status akhir
    status_color = "#4CAF50" if processed_files else "#FF9800"
    status_text = "SUKSES" if processed_files else "TIDAK ADA FILE YANG DIPROSES"
    
    html += f"""
            <div style="margin: 30px 0; padding: 15px; background-color: {status_color}; color: white; border-radius: 5px; text-align: center;">
                <h3>STATUS AKHIR: {status_text}</h3>
                <p>Workflow akan dianggap sukses selama tidak ada error sistem.</p>
            </div>
            
            <div class="footer">
                <p><em>Email ini dikirim secara otomatis oleh sistem Verval Pupuk 2.0</em></p>
                <p><small>⚠ Warning: File tidak diproses karena tidak memiliki kolom TGL INPUT atau format tidak sesuai.</small></p>
                <p><small>✅ Sukses: File berhasil diproses dan di-rename sesuai bulan.</small></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

# ----------------------------------------------------
# MAIN
# ----------------------------------------------------

def main():
    try:
       def main():
    try:
        add_log("🚀 Memulai proses Excel Verval Pupuk")
        
        # Validasi konfigurasi
        validate_config()
        
        # Autentikasi
        drive = authenticate_drive()
        
        # List file
        files = list_files_in_folder(drive, FOLDER_ID)

        if not files:
            add_log("ℹ️ Tidak ada file Excel di folder sumber.")
            send_email_notification(
                subject="[Verval Pupuk] Tidak Ada File untuk Diproses",
                body=f"<p>Tidak ditemukan file Excel di folder sumber pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</p>"
            )
            return 0  # ✅ Exit code sukses meski tidak ada file

        add_log(f"📂 Ditemukan {len(files)} file Excel")
        
        # Proses setiap file
        for f in files:
            result = process_excel(drive, f["id"], f["name"])
            if result:
                processed_files.append(result)
        
        # Kirim notifikasi email
        email_body = create_email_body(processed_files, error_messages)
        
        # Tentukan subject berdasarkan hasil
        if error_messages:
            subject = f"[Verval Pupuk] Proses Selesai dengan {len(error_messages)} Warning"
        elif processed_files:
            subject = f"[Verval Pupuk] Berhasil Memproses {len(processed_files)} File"
        else:
            subject = "[Verval Pupuk] Proses Selesai (Tidak Ada File Diproses)"
        
        send_email_notification(subject, email_body)
        
        # LOGIKA EXIT CODE YANG BARU
        if processed_files:
            add_log(f"🎉 Proses selesai! {len(processed_files)} file berhasil diproses, {len(error_messages)} warning")
            return 0  # ✅ Exit code sukses jika ada minimal 1 file berhasil
        else:
            add_log("ℹ️ Tidak ada file yang berhasil diproses")
            return 0  # ✅ Tetap exit code sukses, karena ini bukan error sistem
        
    except Exception as e:
        error_msg = f"❌ Error utama: {str(e)}"
        add_log(error_msg, is_error=True)
        
        # Kirim email error
        error_email_body = f"""
        <h2>❌ Error Proses Excel Verval Pupuk</h2>
        <p>Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</p>
        <p>Error: {str(e)}</p>
        <p>Harap periksa logs di GitHub Actions untuk detail lebih lanjut.</p>
        """
        
        send_email_notification(
            subject="[Verval Pupuk] ERROR - Proses Gagal",
            body=error_email_body
        )
        
        return 1  # ❌ Exit code error hanya untuk exception sistem

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
