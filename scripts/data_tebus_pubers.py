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
from datetime import datetime, timedelta
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============================
# KONFIGURASI OPTIMASI DATA BESAR
# ============================
FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"  # Folder Google Drive
SAVE_FOLDER = "data_bulanan"  # Folder lokal di runner
SPREADSHEET_ID = "1wcfplBgnpZmYZR-I6p774DZKBjz8cG326F8Z_EK4KDM"
SHEET_NAME = "Rekap_Gabungan"

# Optimasi untuk data besar
BATCH_SIZE = 3000  # Ukuran batch untuk API requests
MAX_RETRIES = 3  # Maks percobaan retry
RETRY_DELAY = 2  # Delay antar retry (detik)
BUFFER_ROWS = 1000  # Buffer untuk resize worksheet

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
# FUNGSI KONVERSI TANGGAL (DIPERBAIKI - hanya dd-mm-yyyy)
# ============================
def parse_tanggal_tebus(tanggal_str):
    """
    Mengonversi string tanggal menjadi format dd-mm-yyyy
    Menghapus bagian waktu jika ada
    """
    if pd.isna(tanggal_str) or tanggal_str is None or tanggal_str == "":
        return None
    
    try:
        tanggal_str = str(tanggal_str).strip()
        
        # Jika sudah datetime object
        if isinstance(tanggal_str, datetime):
            return tanggal_str.strftime('%d-%m-%Y')
        
        # Coba berbagai format dan konversi ke dd-mm-yyyy
        # Format yyyy-mm-dd HH:MM:SS
        if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', tanggal_str):
            dt = datetime.strptime(tanggal_str, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%d-%m-%Y')
        
        # Format yyyy-mm-dd
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', tanggal_str):
            dt = datetime.strptime(tanggal_str, '%Y-%m-%d')
            return dt.strftime('%d-%m-%Y')
        
        # Format dd-mm-yyyy HH:MM:SS
        elif re.match(r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$', tanggal_str):
            dt = datetime.strptime(tanggal_str, '%d-%m-%Y %H:%M:%S')
            return dt.strftime('%d-%m-%Y')
        
        # Format dd-mm-yyyy (sudah benar)
        elif re.match(r'^\d{2}-\d{2}-\d{4}$', tanggal_str):
            return tanggal_str
        
        # Format dd/mm/yyyy
        elif re.match(r'^\d{2}/\d{2}/\d{4}$', tanggal_str):
            dt = datetime.strptime(tanggal_str, '%d/%m/%Y')
            return dt.strftime('%d-%m-%Y')
        
        # Format Excel serial number (angka)
        elif tanggal_str.replace('.', '').isdigit():
            try:
                # Konversi dari Excel serial number
                excel_date = float(tanggal_str)
                dt = datetime(1899, 12, 30) + timedelta(days=excel_date)
                return dt.strftime('%d-%m-%Y')
            except:
                return None
        else:
            print(f"⚠️  Format tanggal tidak dikenali: {tanggal_str}")
            return None
            
    except Exception as e:
        print(f"⚠️  Error parsing tanggal '{tanggal_str}': {str(e)}")
        return None

def format_tanggal_display(tanggal_str):
    """
    Format tanggal untuk ditampilkan (selalu dd-mm-yyyy)
    """
    formatted = parse_tanggal_tebus(tanggal_str)
    return formatted if formatted else ""

# ============================
# FUNGSI URUTKAN DATA BERDASARKAN BULAN DAN TANGGAL
# ============================
def urutkan_data_per_nik(group):
    """
    Mengurutkan data dalam group NIK berdasarkan tanggal (dd-mm-yyyy)
    """
    group = group.copy()
    
    # Parse dan format tanggal
    group['TGL_TEBS_FORMATTED'] = group['TGL TEBUS'].apply(parse_tanggal_tebus)
    group['TGL_TEBS_DATETIME'] = group['TGL TEBUS'].apply(lambda x: 
        datetime.strptime(parse_tanggal_tebus(x), '%d-%m-%Y') 
        if parse_tanggal_tebus(x) else None
    )
    
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
# FUNGSI OPTIMASI UNTUK DATA BESAR (200K+ BARIS)
# ============================
def optimize_worksheet_for_large_data(worksheet, required_rows, required_cols):
    """
    Optimasi worksheet untuk menampung data besar
    """
    try:
        current_rows = worksheet.row_count
        current_cols = worksheet.col_count
        
        print(f"📐 Ukuran worksheet saat ini: {current_rows:,} baris x {current_cols} kolom")
        print(f"📐 Ukuran data yang dibutuhkan: {required_rows:,} baris x {required_cols} kolom")
        
        # Hitung ukuran baru dengan buffer untuk masa depan
        new_rows = max(current_rows, required_rows + BUFFER_ROWS)
        new_cols = max(current_cols, required_cols + 2)  # +2 kolom buffer
        
        # Jika perlu resize
        if new_rows > current_rows or new_cols > current_cols:
            print(f"🔄 Resizing worksheet ke {new_rows:,} baris x {new_cols} kolom...")
            worksheet.resize(rows=new_rows, cols=new_cols)
            print(f"✅ Worksheet berhasil di-resize")
            time.sleep(1)  # Tunggu API
            
        return True
        
    except Exception as e:
        print(f"⚠️  Gagal mengoptimasi worksheet: {str(e)}")
        return False

def write_large_dataset_to_sheet(worksheet, dataframe, batch_size=BATCH_SIZE):
    """
    Menulis dataset besar ke worksheet dengan chunking dan retry mechanism
    """
    try:
        print(f"📤 Memulai penulisan {len(dataframe):,} baris ke Google Sheets...")
        
        # 1. Optimasi worksheet
        required_rows = len(dataframe) + 1  # +1 untuk header
        required_cols = len(dataframe.columns)
        optimize_worksheet_for_large_data(worksheet, required_rows, required_cols)
        
        # 2. Clear existing data
        print("🧹 Membersihkan data lama...")
        try:
            worksheet.clear()
            time.sleep(1)
        except Exception as e:
            print(f"⚠️  Warning saat clear worksheet: {str(e)}")
        
        # 3. Siapkan data
        print("📦 Menyiapkan data untuk ditulis...")
        data_to_update = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
        total_rows = len(data_to_update)
        
        print(f"📊 Total data untuk ditulis: {total_rows:,} baris")
        
        # 4. Hitung jumlah chunk
        num_chunks = (total_rows + batch_size - 1) // batch_size
        print(f"🔀 Data akan dibagi menjadi {num_chunks} chunk ({batch_size:,} baris per chunk)")
        
        # 5. Proses setiap chunk dengan retry mechanism
        for chunk_idx in range(num_chunks):
            start_row = chunk_idx * batch_size
            end_row = min(start_row + batch_size, total_rows)
            current_chunk = data_to_update[start_row:end_row]
            
            chunk_success = False
            retry_count = 0
            
            while not chunk_success and retry_count < MAX_RETRIES:
                try:
                    start_cell = f"A{start_row + 1}"
                    
                    if retry_count > 0:
                        print(f"   🔄 Retry {retry_count} untuk chunk {chunk_idx + 1}")
                        time.sleep(RETRY_DELAY * (retry_count + 1))
                    
                    worksheet.update(
                        range_name=start_cell,
                        values=current_chunk,
                        value_input_option='USER_ENTERED'
                    )
                    
                    chunk_success = True
                    print(f"   ✅ Chunk {chunk_idx + 1}/{num_chunks}: baris {start_row + 1:,}-{end_row:,}")
                    
                    # Jeda antar chunk (kecuali chunk terakhir)
                    if chunk_idx < num_chunks - 1:
                        time.sleep(0.5)
                        
                except Exception as chunk_error:
                    retry_count += 1
                    error_msg = str(chunk_error)
                    print(f"   ❌ Error pada chunk {chunk_idx + 1} (attempt {retry_count}): {error_msg[:100]}...")
                    
                    if "rate limit" in error_msg.lower():
                        print(f"   ⏳ Rate limit terdeteksi, menunggu {RETRY_DELAY * 3} detik...")
                        time.sleep(RETRY_DELAY * 3)
                    
                    if retry_count >= MAX_RETRIES:
                        print(f"   ⚠️  Gagal menulis chunk {chunk_idx + 1} setelah {MAX_RETRIES} percobaan")
                        raise
        
        print(f"🎉 Berhasil menulis semua data! Total {total_rows:,} baris")
        return True
        
    except Exception as e:
        print(f"❌ Gagal menulis data ke Google Sheets: {str(e)}")
        raise

# ============================
# FUNGSI UTAMA YANG DIPERBAIKI
# ============================
def main():
    try:
        log = []
        all_data = []
        total_rows = 0
        file_count = 0
        nik_cleaning_log = []

        print("=" * 70)
        print("🚀 MEMULAI PROSES REKAP DATA (Optimized for Large Data)")
        print("=" * 70)
        print(f"📧 Email pengirim: {SENDER_EMAIL}")
        print(f"📧 Email penerima: {', '.join(recipient_list[:3])}{'...' if len(recipient_list) > 3 else ''}")
        print(f"⚙️  Batch Size: {BATCH_SIZE:,} baris")
        print(f"⚙️  Max Retries: {MAX_RETRIES}")
        print(f"📅 Format Tanggal: dd-mm-yyyy")
        print()

        # 1. Download semua Excel
        excel_files = download_excel_files(FOLDER_ID)
        print(f"📁 Berhasil download {len(excel_files)} file Excel")
        print()

        # 2. Proses setiap file dengan optimasi memory
        for fpath in excel_files:
            file_count += 1
            filename = os.path.basename(fpath)
            print(f"🔄 Memproses file {file_count}/{len(excel_files)}: {filename}")
            
            try:
                # Load dengan dtype string untuk menghemat memory
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
                if len(nik_cleaning_log) < 20:  # Simpan hanya 20 contoh
                    nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")

            # Hapus baris dengan NIK kosong
            df = df[df['NIK'].notna()]
            cleaned_nik_count = len(df)

            # Format tanggal ke dd-mm-yyyy
            if 'TGL TEBUS' in df.columns:
                df['TGL TEBUS'] = df['TGL TEBUS'].apply(format_tanggal_display)

            total_rows += cleaned_nik_count
            log.append(f"- {filename}: {original_nik_count:,} → {cleaned_nik_count:,} baris")
            all_data.append(df)
            
            print(f"   ✅ Berhasil: {original_nik_count:,} → {cleaned_nik_count:,} baris")
            
            # Free memory
            del df

        print()
        
        if not all_data:
            raise ValueError("❌ Tidak ada data yang berhasil diproses dari semua file")

        # 3. Gabungkan semua data
        print("🔄 Menggabungkan data dari semua file...")
        combined = pd.concat(all_data, ignore_index=True)
        print(f"✅ Total data gabungan: {len(combined):,} baris")
        
        # Free memory
        del all_data

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
        print(f"📊 Jumlah NIK unik yang akan diproses: {combined['NIK'].nunique():,}")
        
        output_rows = []
        
        # Progress tracking
        total_unique_niks = combined['NIK'].nunique()
        processed_niks = 0
        progress_interval = max(1, total_unique_niks // 20)  # Update progress setiap 5%
        
        for nik, group in combined.groupby("NIK"):
            processed_niks += 1
            
            # Show progress
            if processed_niks % progress_interval == 0:
                progress = (processed_niks / total_unique_niks) * 100
                print(f"   📈 Progress: {processed_niks:,}/{total_unique_niks:,} NIK ({progress:.1f}%)")
            
            # Urutkan data dalam group
            group_sorted = urutkan_data_per_nik(group)
            
            if len(group_sorted) == 0:
                continue
                
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
        print(f"✅ Rekap selesai: {len(out_df):,} NIK unik ditemukan")
        
        # Free memory
        del combined

        # 6. Tulis ke Google Sheet dengan optimasi data besar
        print()
        print("=" * 70)
        print("📤 MENULIS DATA KE GOOGLE SHEETS")
        print("=" * 70)
        
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        # Cek atau buat worksheet
        try:
            ws = sh.worksheet(SHEET_NAME)
            print(f"✅ Sheet '{SHEET_NAME}' ditemukan")
        except gspread.exceptions.WorksheetNotFound:
            print(f"⚠️  Sheet '{SHEET_NAME}' tidak ditemukan, membuat baru...")
            # Buat dengan ukuran yang cukup untuk data besar
            initial_rows = max(200000, len(out_df) * 2)
            initial_cols = len(out_df.columns) + 2
            ws = sh.add_worksheet(
                title=SHEET_NAME, 
                rows=initial_rows, 
                cols=initial_cols
            )
            print(f"✅ Sheet '{SHEET_NAME}' berhasil dibuat ({initial_rows:,} baris)")
        
        # Tulis data dengan fungsi yang dioptimasi
        write_large_dataset_to_sheet(ws, out_df)

        # 7. Buat laporan sukses
        print()
        print("=" * 70)
        print("✅ PROSES SELESAI DENGAN OPTIMASI DATA BESAR")
        print("=" * 70)
        
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        success_message = f"""
REKAP DATA BERHASIL DENGAN OPTIMASI DATA BESAR ✓

📅 Tanggal Proses: {now}
📁 File Diproses: {file_count}
📊 Total Data Awal: {total_rows:,} baris
👥 Unique NIK: {len(out_df):,}
🔧 NIK Dibersihkan: {len(nik_cleaning_log)} entri
⚙️  Batch Size: {BATCH_SIZE:,} baris
🔄 Max Retries: {MAX_RETRIES}
📅 Format Tanggal: dd-mm-yyyy

📋 DETAIL FILE:
{chr(10).join(log)}

🔍 CONTOH NIK YANG DIBERSIHKAN (10 pertama):
{chr(10).join(nik_cleaning_log[:10])}
{"... (masih ada " + str(len(nik_cleaning_log) - 10) + " entri lainnya)" if len(nik_cleaning_log) > 10 else ""}

✅ DATA TELAH BERHASIL DIUPLOAD:
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📄 Sheet: {SHEET_NAME}
📈 Baris Data: {len(out_df):,}
📏 Ukuran Worksheet: {ws.row_count:,} baris x {ws.col_count} kolom

🔧 OPTIMASI YANG DITERAPKAN:
1. Batch processing ({BATCH_SIZE:,} baris per batch)
2. Automatic worksheet resizing
3. Retry mechanism ({MAX_RETRIES}x retry)
4. Rate limit handling
5. Progress tracking untuk data besar
6. Memory optimization
7. Format tanggal konsisten: dd-mm-yyyy

📍 REPOSITORY: verval-pupuk2/scripts/data_tebus_pubers.py
⚡ TELAH DIOPTIMASI UNTUK DATA HINGGA 200,000+ BARIS
"""

        print(f"📊 Ringkasan: {now}, File: {file_count}, Data: {total_rows:,}, NIK: {len(out_df):,}")

        # 8. Kirim email notifikasi sukses
        print("📧 Mengirim notifikasi email...")
        send_email_notification("REKAP DATA BERHASIL (Optimized Large Data)", success_message, is_success=True)
        
        print("\n" + "=" * 70)
        print("🎉 SEMUA PROSES TELAH BERHASIL!")
        print("=" * 70)
        
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

📊 DATA STATS:
- File diproses: {file_count}
- Total baris: {total_rows:,}
- Unique NIK: {len(out_df) if 'out_df' in locals() else 0:,}

🔧 TROUBLESHOOTING:
1. Pastikan service account punya akses EDITOR di Google Sheet
2. Cek apakah spreadsheet ID benar: {SPREADSHEET_ID}
3. Service Account: github-verval-pupuk@verval-pupuk-automation.iam.gserviceaccount.com
4. Batch Size saat ini: {BATCH_SIZE:,} baris
5. Worksheet size limit: 10 juta sel

🔧 TRACEBACK:
{traceback.format_exc()[:500]}... (truncated)
"""
        print("\n" + "=" * 70)
        print("❌ REKAP GAGAL")
        print("=" * 70)
        print(error_message)

        # Kirim email notifikasi error
        send_email_notification("REKAP DATA GAGAL", error_message, is_success=False)
        return False

# ============================
# JALANKAN PROSES UTAMA
# ============================
if __name__ == "__main__":
    start_time = time.time()
    print("⏱️  Memulai proses...")
    
    success = main()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n⏱️  Durasi proses: {duration:.2f} detik")
    print(f"⏱️  Durasi: {duration//60:.0f} menit {duration%60:.0f} detik")
    
    if success:
        print("✅ Script berhasil dijalankan!")
        exit(0)
    else:
        print("❌ Script gagal dijalankan!")
        exit(1)
