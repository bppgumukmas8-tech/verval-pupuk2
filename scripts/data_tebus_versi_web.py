"""
data_tebus_versi_web.py
Script untuk cleaning dan reordering data Verval Pupuk untuk versi web.
Proses: membersihkan NIK dan mengubah urutan kolom.

Lokasi: verval-pupuk2/scripts/data_tebus_versi_web.py
"""

import os
import sys
import pandas as pd
import gspread
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import traceback
import json
import io

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "1AXQdEUW1dXRcdT0m0QkzvT7ZJjN0Vt4E"  # Folder Google Drive

# GUNAKAN SPREADSHEET YANG SAMA DENGAN WORKFLOW LAIN
SPREADSHEET_ID = "1kh9OBcSKrh_cDy6u071vQP1kkEXNdpM4ERP9rd0tjqw"
DATA_SHEET_NAME = "Data_Gabungan"  # Nama sheet untuk data
INFO_SHEET_NAME = "Sheet1"  # Nama sheet untuk info update

# ============================
# LOAD EMAIL CONFIGURATION FROM SECRETS
# ============================
def load_email_config():
    """
    Memuat konfigurasi email dari environment variables/secrets
    """
    # Load dari environment variables
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
    RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")
    
    # Validasi
    if not SENDER_EMAIL:
        raise ValueError("❌ SECRET SENDER_EMAIL TIDAK TERBACA")
    if not SENDER_EMAIL_PASSWORD:
        raise ValueError("❌ SECRET SENDER_EMAIL_PASSWORD TIDAK TERBACA")
    if not RECIPIENT_EMAILS:
        raise ValueError("❌ SECRET RECIPIENT_EMAILS TIDAK TERBACA")
    
    # Parse recipient emails
    try:
        # Coba parse sebagai JSON array
        recipient_list = json.loads(RECIPIENT_EMAILS)
    except json.JSONDecodeError:
        # Jika bukan JSON, split berdasarkan koma
        recipient_list = [email.strip() for email in RECIPIENT_EMAILS.split(",")]
    
    return {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": SENDER_EMAIL,
        "sender_password": SENDER_EMAIL_PASSWORD,
        "recipient_emails": recipient_list
    }

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
# FUNGSI FORMAT TANGGAL
# ============================
def format_tanggal(tanggal_value):
    """
    Format tanggal menjadi dd-mm-yyyy
    Menangani berbagai format input
    """
    if pd.isna(tanggal_value) or tanggal_value is None:
        return ""
    
    try:
        # Coba parse sebagai datetime pandas
        if isinstance(tanggal_value, pd.Timestamp):
            return tanggal_value.strftime('%d-%m-%Y')
        
        # Convert ke string
        tanggal_str = str(tanggal_value).strip()
        
        # Jika kosong, return string kosong
        if not tanggal_str:
            return ""
        
        # Coba parse dengan berbagai format
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%y', '%d/%m/%y']:
            try:
                dt = datetime.strptime(tanggal_str, fmt)
                return dt.strftime('%d-%m-%Y')
            except ValueError:
                continue
        
        # Jika tidak bisa di-parse, return as-is (tapi bersihkan whitespace)
        return tanggal_str
        
    except Exception as e:
        print(f"⚠️  Gagal format tanggal '{tanggal_value}': {e}")
        return str(tanggal_value).strip()

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """
    Mengirim notifikasi email tentang status proses
    """
    try:
        # Load config email
        EMAIL_CONFIG = load_email_config()
        
        # Konfigurasi email
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        msg['Subject'] = f"[verval-pupuk2] {subject}"
        
        # Style untuk email
        if is_success:
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: green;">✅ {subject}</h2>
                    <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                        {message.replace(chr(10), '<br>')}
                    </div>
                    <p><small>📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py</small></p>
                    <p><small>⏰ Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
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
                    <p><small>📁 Repository: verval-pupuk2/scripts/data_tebus_versi_web.py</small></p>
                    <p><small>⏰ Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</small></p>
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
# FUNGSI DOWNLOAD FILE
# ============================
def download_excel_files(folder_id, save_folder="data_web"):
    """
    Download file Excel dari Google Drive
    """
    os.makedirs(save_folder, exist_ok=True)
    
    # Load credentials
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan")
    
    credentials = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # Query untuk mencari file Excel
    query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ Tidak ada file Excel di folder Google Drive.")

    paths = []
    for f in files:
        print(f"📥 Downloading: {f['name']}")
        request = drive_service.files().get_media(fileId=f["id"])
        file_path = os.path.join(save_folder, f["name"])
        
        with io.FileIO(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        
        paths.append({
            'path': file_path,
            'name': f['name']
        })
    
    print(f"✅ Berhasil download {len(paths)} file Excel")
    return paths

# ============================
# FUNGSI BERSIHKAN SHEET
# ============================
def clear_sheet_contents(worksheet):
    """
    Membersihkan semua isi sheet sepenuhnya
    """
    try:
        print(f"   🧹 Membersihkan sheet '{worksheet.title}'...")
        
        # Method 1: Coba gunakan clear() untuk membersihkan semua
        worksheet.clear()
        print(f"   ✅ Sheet berhasil dibersihkan dengan clear()")
        
        # Method 2: Untuk memastikan, tambahkan batch clear
        try:
            # Dapatkan dimensi sheet
            row_count = worksheet.row_count
            col_count = worksheet.col_count
            
            if row_count > 0 and col_count > 0:
                # Buat data kosong dengan ukuran yang sama
                empty_data = [['' for _ in range(col_count)] for _ in range(row_count)]
                worksheet.update('A1', empty_data)
                print(f"   ✅ Sheet dikosongkan dengan update data kosong")
        except Exception as e2:
            print(f"   ⚠️  Batch clear tidak diperlukan: {str(e2)}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Gagal membersihkan sheet: {str(e)}")
        # Coba metode alternatif
        try:
            # Reset sheet dengan menghapus dan membuat baru
            spreadsheet = worksheet.spreadsheet
            sheet_id = worksheet.id
            
            # Hapus sheet
            spreadsheet.del_worksheet(worksheet)
            
            # Buat sheet baru dengan nama yang sama
            new_worksheet = spreadsheet.add_worksheet(
                title=worksheet.title, 
                rows=1000, 
                cols=worksheet.col_count
            )
            print(f"   ✅ Sheet direset dengan menghapus dan membuat baru")
            return new_worksheet
        except Exception as e2:
            print(f"   ❌ Gagal reset sheet: {str(e2)}")
            return False

# ============================
# FUNGSI UPDATE INFO DI SHEET1
# ============================
def update_info_sheet(ws_info, update_date, update_time, file_count, total_rows, unique_nik):
    """
    Update informasi update di Sheet1
    """
    try:
        print(f"   🧹 Membersihkan Sheet1 sebelum update...")
        clear_sheet_contents(ws_info)
        
        # Data untuk Sheet1
        info_data = [
            ["DATA UPDATE - VERVAL PUPUK WEB VERSION"],
            ["=" * 50],
            [""],
            ["INFORMASI UPDATE:"],
            ["Data update per tanggal input :"],
            [update_date],
            [f"Jam update: {update_time}"],
            [""],
            ["=" * 50],
            [""],
            ["STATISTIK DATA TERBARU:"],
            [f"• Jumlah File Diproses: {file_count}"],
            [f"• Total Baris Data: {total_rows:,}"],
            [f"• Unique NIK: {unique_nik:,}"],
            [f"• Update Terakhir: {update_date} {update_time}"],
            [""],
            ["PROSES YANG DILAKUKAN:"],
            ["• Download file Excel dari Google Drive"],
            ["• Cleaning NIK (hapus karakter non-digit)"],
            ["• Format tanggal: dd-mm-yyyy"],
            ["• Reordering kolom untuk web"],
            ["• Upload ke Google Sheets"],
            [""],
            ["INFORMASI SISTEM:"],
            ["• Sistem berjalan otomatis tiap hari"],
            ["• Update harian pukul 15:00 WIB"],
            ["• Notifikasi via email otomatis"],
            ["• Repository: verval-pupuk2/scripts"],
            [""],
            ["KONTAK & DUKUNGAN:"],
            ["• Untuk pertanyaan hubungi admin"],
            ["• Laporkan masalah jika ditemukan"],
            [""],
            [f"© {datetime.now().year} - Sistem Verval Pupuk Web Version"]
        ]
        
        # Update data ke Sheet1
        for i, row in enumerate(info_data, start=1):
            ws_info.update(f'A{i}', [row])
        
        # Formatting untuk Sheet1
        # Header utama
        ws_info.format('A1', {
            "backgroundColor": {
                "red": 0.2,
                "green": 0.6, 
                "blue": 0.9
            },
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "fontSize": 16,
                "bold": True
            },
            "borders": {
                "top": {"style": "SOLID", "width": 2},
                "bottom": {"style": "SOLID", "width": 2},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            }
        })
        
        # Judul informasi update
        ws_info.format('A4', {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.95,
                "blue": 1.0
            },
            "textFormat": {
                "fontSize": 13,
                "bold": True
            }
        })
        
        # Info tanggal update
        ws_info.format('A5:A7', {
            "backgroundColor": {
                "red": 0.98,
                "green": 0.98,
                "blue": 0.98
            },
            "textFormat": {
                "fontSize": 11
            }
        })
        
        # Tanggal spesifik
        ws_info.format('A6', {
            "backgroundColor": {
                "red": 1.0,
                "green": 0.9,
                "blue": 0.8
            },
            "textFormat": {
                "fontSize": 12,
                "bold": True,
                "foregroundColor": {
                    "red": 0.8,
                    "green": 0.4,
                    "blue": 0.0
                }
            }
        })
        
        # Jam update
        ws_info.format('A7', {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.95,
                "blue": 1.0
            },
            "textFormat": {
                "fontSize": 11,
                "italic": True,
                "foregroundColor": {
                    "red": 0.3,
                    "green": 0.3,
                    "blue": 0.3
                }
            }
        })
        
        # Judul statistik
        ws_info.format('A11', {
            "backgroundColor": {
                "red": 0.8,
                "green": 0.9,
                "blue": 0.8
            },
            "textFormat": {
                "fontSize": 13,
                "bold": True
            }
        })
        
        # Data statistik
        ws_info.format('A12:A15', {
            "backgroundColor": {
                "red": 0.95,
                "green": 0.95,
                "blue": 0.95
            },
            "textFormat": {
                "fontSize": 11
            }
        })
        
        # Judul proses
        ws_info.format('A17', {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.9,
                "blue": 0.8
            },
            "textFormat": {
                "fontSize": 13,
                "bold": True
            }
        })
        
        # Daftar proses
        ws_info.format('A18:A23', {
            "backgroundColor": {
                "red": 0.98,
                "green": 0.98,
                "blue": 0.98
            },
            "textFormat": {
                "fontSize": 11
            }
        })
        
        # Judul informasi sistem
        ws_info.format('A25', {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.8,
                "blue": 0.9
            },
            "textFormat": {
                "fontSize": 13,
                "bold": True
            }
        })
        
        # Informasi sistem
        ws_info.format('A26:A30', {
            "backgroundColor": {
                "red": 0.95,
                "green": 0.95,
                "blue": 0.95
            },
            "textFormat": {
                "fontSize": 11
            }
        })
        
        # Judul kontak
        ws_info.format('A32', {
            "backgroundColor": {
                "red": 0.8,
                "green": 0.8,
                "blue": 0.9
            },
            "textFormat": {
                "fontSize": 13,
                "bold": True
            }
        })
        
        # Kontak
        ws_info.format('A33:A34', {
            "backgroundColor": {
                "red": 0.95,
                "green": 0.95,
                "blue": 0.95
            },
            "textFormat": {
                "fontSize": 11
            }
        })
        
        # Footer
        ws_info.format('A36', {
            "backgroundColor": {
                "red": 0.2,
                "green": 0.6,
                "blue": 0.9
            },
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "fontSize": 10,
                "italic": True
            },
            "borders": {
                "top": {"style": "SOLID_THICK", "color": {"red": 0.2, "green": 0.6, "blue": 0.9}}
            }
        })
        
        # Auto-resize kolom A
        try:
            ws_info.columns_auto_resize(0, 0)  # Hanya resize kolom A
        except:
            print("   ⚠️  Auto-resize tidak tersedia, lewati...")
        
        print("   ✅ Sheet1 berhasil diupdate dengan informasi terbaru")
        return True
        
    except Exception as e:
        print(f"   ⚠️  Gagal update Sheet1: {str(e)}")
        return False

# ============================
# FUNGSI FORMAT DATA SHEET
# ============================
def format_data_sheet(ws_data):
    """
    Format sheet Data_Gabungan (hanya header)
    """
    try:
        # Format header (baris 1)
        header_range = f'A1:N1'
        
        # Format untuk header
        ws_data.format(header_range, {
            "backgroundColor": {
                "red": 0.2,
                "green": 0.6, 
                "blue": 0.9
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "fontSize": 11,
                "bold": True
            },
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            }
        })
        
        # Format untuk baris data (ganjil)
        if ws_data.row_count > 1:
            # Format baris genap (agar lebih mudah dibaca)
            try:
                # Hanya format beberapa baris pertama untuk efisiensi
                max_rows_to_format = min(1000, ws_data.row_count)
                for row in range(2, max_rows_to_format + 1, 2):  # Baris genap
                    ws_data.format(f'A{row}:N{row}', {
                        "backgroundColor": {
                            "red": 0.98,
                            "green": 0.98,
                            "blue": 0.98
                        }
                    })
            except:
                pass
        
        # Auto-resize semua kolom
        try:
            ws_data.columns_auto_resize(0, 13)  # Resize kolom A sampai N
        except:
            print("   ⚠️  Auto-resize tidak tersedia, lewati...")
        
        print("   ✅ Formatting Data_Gabungan berhasil")
        return True
        
    except Exception as e:
        print(f"   ⚠️  Gagal formatting Data_Gabungan: {str(e)}")
        return False

# ============================
# FUNGSI UTAMA
# ============================
def process_data_for_web():
    """
    Fungsi utama untuk processing data versi web
    """
    print("=" * 60)
    print("🚀 PROSES CLEANING & REORDERING DATA UNTUK WEB")
    print("=" * 60)
    print(f"📁 Script: data_tebus_versi_web.py")
    print(f"📂 Folder ID: {FOLDER_ID}")
    print(f"📊 Spreadsheet ID: {SPREADSHEET_ID}")
    print(f"📄 Sheet Data: {DATA_SHEET_NAME}")
    print(f"📄 Sheet Info: {INFO_SHEET_NAME}")
    print("=" * 60)
    
    try:
        log = []
        all_data = []
        total_rows = 0
        file_count = 0
        nik_cleaning_log = []
        tanggal_format_log = []

        print("🔍 Memulai proses cleaning dan reordering data...")
        
        # ========== LOAD CREDENTIALS ==========
        creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("❌ SECRET GOOGLE_APPLICATION_CREDENTIALS_JSON TIDAK TERBACA")

        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )

        gc = gspread.authorize(credentials)
        
        # Download semua Excel
        excel_files = download_excel_files(FOLDER_ID, save_folder="data_web")
        print(f"📁 Berhasil download {len(excel_files)} file Excel")

        # Proses setiap file Excel
        for file_info in excel_files:
            file_count += 1
            fpath = file_info['path']
            filename = file_info['name']
            
            print(f"\n📖 Memproses: {filename}")
            
            try:
                df = pd.read_excel(fpath, dtype=str)  # pastikan NIK terbaca full string
                
                # PROSES BERSIHKAN NIK
                original_nik_count = len(df)
                df['NIK_ORIGINAL'] = df['NIK']  # Simpan nilai asli untuk logging
                df['NIK'] = df['NIK'].apply(clean_nik)
                
                # Log NIK yang dibersihkan
                cleaned_niks = df[df['NIK_ORIGINAL'] != df['NIK']][['NIK_ORIGINAL', 'NIK']]
                for _, row in cleaned_niks.iterrows():
                    nik_cleaning_log.append(f"'{row['NIK_ORIGINAL']}' -> {row['NIK']}")
                
                # PROSES FORMAT TANGGAL TEBUS
                if 'TGL TEBUS' in df.columns:
                    # Simpan nilai asli untuk logging
                    df['TGL_TEBUS_ORIGINAL'] = df['TGL TEBUS']
                    
                    # Apply formatting function
                    df['TGL TEBUS'] = df['TGL TEBUS'].apply(format_tanggal)
                    
                    # Log perubahan format tanggal
                    for _, row in df[['TGL_TEBUS_ORIGINAL', 'TGL TEBUS']].iterrows():
                        if str(row['TGL_TEBUS_ORIGINAL']).strip() != str(row['TGL TEBUS']).strip():
                            tanggal_format_log.append(f"'{row['TGL_TEBUS_ORIGINAL']}' -> {row['TGL TEBUS']}")
                
                # Hapus kolom sementara
                df = df.drop(columns=['NIK_ORIGINAL', 'TGL_TEBUS_ORIGINAL'], errors='ignore')
                
                # Hapus baris dengan NIK kosong setelah cleaning
                df = df[df['NIK'].notna()]
                cleaned_nik_count = len(df)
                
                total_rows += cleaned_nik_count
                log.append(f"- {filename}: {original_nik_count} -> {cleaned_nik_count} baris (setelah cleaning NIK)")
                
                # Pastikan kolom pupuk bertipe numeric
                pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']
                for col in pupuk_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                all_data.append(df)
                print(f"   ✅ Berhasil: {cleaned_nik_count} baris")
                
            except Exception as e:
                print(f"   ❌ Error memproses {filename}: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            print(f"❌ ERROR: {error_msg}")
            send_email_notification("CLEANING DATA WEB GAGAL", error_msg, is_success=False)
            return False

        # Gabungkan semua data
        combined = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan: {len(combined):,} baris")

        # Pastikan kolom sesuai header
        original_columns = [
            'KECAMATAN', 'NO TRANSAKSI', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
            'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
            'TGL TEBUS', 'STATUS'
        ]
        
        # Handle jika ada kolom yang missing
        for col in original_columns:
            if col not in combined.columns:
                combined[col] = ""
        
        combined = combined[original_columns]

        # REORDER KOLOM sesuai permintaan
        new_column_order = [
            'NIK',           # (1) - dari (4)
            'NAMA PETANI',   # (2) - dari (5)  
            'KECAMATAN',     # (3) - dari (1)
            'NAMA KIOS',     # (4) - dari (3)
            'NO TRANSAKSI',  # (5) - dari (2)
            'UREA',          # (6) - tetap (6)
            'NPK',           # (7) - tetap (7)
            'SP36',          # (8) - tetap (8)
            'ZA',            # (9) - tetap (9)
            'NPK FORMULA',   # (10) - tetap (10)
            'ORGANIK',       # (11) - tetap (11)
            'ORGANIK CAIR',  # (12) - tetap (12)
            'TGL TEBUS',     # (13) - tetap (13)
            'STATUS'         # (14) - tetap (14)
        ]
        
        # Apply reordering
        combined_df = combined[new_column_order]

        # Hitung unique NIK
        unique_nik_count = combined_df['NIK'].nunique()

        # Waktu update
        update_time = datetime.now()
        update_date_str = update_time.strftime("%d-%m-%Y")
        update_time_str = update_time.strftime("%H:%M:%S")

        # Tulis ke Google Sheet
        print(f"\n📤 Mengupload data ke Google Sheets...")
        print(f"   Spreadsheet: {SPREADSHEET_ID}")
        print(f"   Sheet Data: {DATA_SHEET_NAME}")
        print(f"   Sheet Info: {INFO_SHEET_NAME}")
        print(f"   Update: {update_date_str} {update_time_str}")
        
        try:
            sh = gc.open_by_key(SPREADSHEET_ID)
            
            # ========== PROSES SHEET1 (INFO) ==========
            print(f"\n📝 Memproses {INFO_SHEET_NAME} untuk info update...")
            try:
                ws_info = sh.worksheet(INFO_SHEET_NAME)
                print(f"   ✅ Sheet '{INFO_SHEET_NAME}' ditemukan")
            except gspread.exceptions.WorksheetNotFound:
                # Buat sheet baru jika tidak ada
                print(f"   📄 Sheet '{INFO_SHEET_NAME}' tidak ditemukan, membuat baru...")
                ws_info = sh.add_worksheet(INFO_SHEET_NAME, rows=50, cols=5)
            
            # Update info di Sheet1
            update_info_sheet(ws_info, update_date_str, update_time_str, 
                            file_count, len(combined_df), unique_nik_count)
            
            # ========== PROSES DATA_GABUNGAN ==========
            print(f"\n📊 Memproses {DATA_SHEET_NAME} untuk data...")
            try:
                ws_data = sh.worksheet(DATA_SHEET_NAME)
                print(f"   ✅ Sheet '{DATA_SHEET_NAME}' ditemukan")
            except gspread.exceptions.WorksheetNotFound:
                # Buat sheet baru jika tidak ada
                print(f"   📄 Sheet '{DATA_SHEET_NAME}' tidak ditemukan, membuat baru...")
                ws_data = sh.add_worksheet(DATA_SHEET_NAME, rows=1000, cols=len(new_column_order))
            
            # BERSIHKAN SHEET SEBELUM MENULIS
            print(f"   🧹 Membersihkan {DATA_SHEET_NAME} sebelum upload data...")
            clear_sheet_contents(ws_data)
            
            # Upload data ke Data_Gabungan
            print(f"   📤 Mengupload data ke {DATA_SHEET_NAME}...")
            
            # PERBAIKAN: Tambahkan header sebagai baris pertama
            # Buat DataFrame dengan header di baris pertama
            print(f"   📝 Menyiapkan data: {len(combined_df):,} baris + 1 baris header")
            
            # Buat DataFrame dengan header
            data_with_header = pd.DataFrame(columns=new_column_order)
            
            # Isi dengan data
            data_with_header = pd.concat([data_with_header, combined_df], ignore_index=True)
            
            # Convert semua nilai ke string untuk menghindari format yang tidak konsisten
            data_with_header = data_with_header.astype(str)
            
            # Upload data dengan set_with_dataframe - TANPA MENAMBAH HEADER LAGI
            # Karena set_with_dataframe akan menulis header otomatis
            print(f"   ⬆️  Uploading {len(data_with_header):,} baris ke Google Sheets...")
            set_with_dataframe(ws_data, data_with_header, include_index=False, include_column_header=True)
            
            # Format data sheet
            format_data_sheet(ws_data)
            
            print(f"   ✅ Data berhasil diupload: {len(combined_df):,} baris × {len(combined_df.columns)} kolom")
            
        except Exception as e:
            print(f"   ❌ Gagal mengupload ke Google Sheets: {str(e)}")
            raise

        # ... (bagian email dan logging tetap sama) ...

    except Exception as e:
        # ... (bagian error handling tetap sama) ...
        return False

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_data_for_web()
