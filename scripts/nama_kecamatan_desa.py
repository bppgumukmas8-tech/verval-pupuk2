"""
nama_kecamatan_desa.py
Script untuk melengkapi dan memverifikasi data kecamatan dan desa pada file ERDKK.

Lokasi: verval-pupuk2/scripts/nama_kecamatan_desa.py
"""

import os
import sys
import pandas as pd
import numpy as np
import json
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import warnings
warnings.filterwarnings('ignore')

# ============================
# KONFIGURASI
# ============================

FOLDER_ID = '13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ'  # ID folder ERDKK
KODE_FILE_ID = '19p-6xUhMfwQ81o37eldJQmSZ7GTtCEJk'  # ID file Kode desa dan kios

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
# FUNGSI UTILITAS
# ============================

def get_service_account_creds():
    """Dapatkan credentials dari environment variable"""
    service_account_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    
    if not service_account_json:
        raise ValueError(
            "❌ GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable tidak ditemukan.\n"
            "Pastikan secret 'GOOGLE_APPLICATION_CREDENTIALS_JSON' sudah ditambahkan di GitHub Secrets."
        )
    
    try:
        # Parse JSON dari environment variable
        service_account_info = json.loads(service_account_json)
        return service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/drive']
        )
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ JSON tidak valid: {str(e)}")
    except Exception as e:
        raise ValueError(f"❌ Gagal membuat credentials: {str(e)}")

def authenticate_drive():
    """Autentikasi ke Google Drive API"""
    try:
        creds = get_service_account_creds()
        service = build('drive', 'v3', credentials=creds)
        print(f"✓ Authenticated as: {creds.service_account_email}")
        return service
    except Exception as e:
        print(f"❌ Authentication failed: {str(e)}")
        raise

def download_file(service, file_id, file_name):
    """Download file dari Google Drive"""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    
    # Simpan file sementara
    with open(file_name, 'wb') as f:
        f.write(fh.read())
    
    return file_name

def update_file(service, file_id, file_path):
    """Update file yang sudah ada di Google Drive (overwrite)"""
    media = MediaFileUpload(
        file_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=True
    )
    
    # Update file yang sudah ada
    updated_file = service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()
    
    return updated_file.get('id')

def rename_file(service, file_id, new_name):
    """Ganti nama file di Google Drive"""
    file_metadata = {
        'name': new_name
    }
    
    updated_file = service.files().update(
        fileId=file_id,
        body=file_metadata,
        fields='id, name'
    ).execute()
    
    return updated_file.get('name')

def get_files_in_folder(service, folder_id):
    """Mendapatkan daftar file dalam folder"""
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = service.files().list(
        q=query,
        pageSize=100,
        fields="nextPageToken, files(id, name)"
    ).execute()
    
    return results.get('files', [])

def clean_filename(filename):
    """Membersihkan nama file dari karakter yang tidak valid"""
    # Hapus karakter yang tidak valid untuk nama file
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
    cleaned = re.sub(invalid_chars, '_', filename)
    # Hapus spasi di awal/akhir
    cleaned = cleaned.strip()
    # Batasi panjang nama file
    if len(cleaned) > 100:
        cleaned = cleaned[:100]
    return cleaned

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
                <body style="font-family: Arial, sans-serif;">
                    <div style="max-width: 800px; margin: 0 auto; padding: 20px; border: 1px solid #4CAF50; border-radius: 5px;">
                        <h2 style="color: #4CAF50;">✅ {subject}</h2>
                        <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px; margin: 20px 0;">
                            {message.replace(chr(10), '<br>')}
                        </div>
                        <p style="color: #777; font-size: 12px; text-align: center;">
                            📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py<br>
                            Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
                        </p>
                    </div>
                </body>
            </html>
            """
        else:
            email_body = f"""
            <html>
                <body style="font-family: Arial, sans-serif;">
                    <div style="max-width: 800px; margin: 0 auto; padding: 20px; border: 1px solid #f44336; border-radius: 5px;">
                        <h2 style="color: #f44336;">❌ {subject}</h2>
                        <div style="background-color: #ffe6e6; padding: 15px; border-radius: 5px; margin: 20px 0;">
                            {message.replace(chr(10), '<br>')}
                        </div>
                        <p style="color: #777; font-size: 12px; text-align: center;">
                            📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py<br>
                            Dikirim secara otomatis pada {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
                        </p>
                    </div>
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

def send_detailed_email_notification(results):
    """Kirim notifikasi email lengkap dengan hasil proses"""
    try:
        # Load config email
        EMAIL_CONFIG = load_email_config()
        
        # Hitung statistik
        total_files = len(results)
        success_files = sum(1 for r in results if r['status'] == 'SUCCESS')
        failed_files = total_files - success_files
        total_rows = sum(r['rows_processed'] for r in results)
        total_kecamatan_filled = sum(r['kecamatan_filled'] for r in results)
        total_kecamatan_updated = sum(r['kecamatan_updated'] for r in results)
        total_desa_filled = sum(r['desa_filled'] for r in results)
        total_desa_updated = sum(r['desa_updated'] for r in results)
        
        # Buat detail file
        file_details = ""
        for r in results:
            status_icon = "✅" if r['status'] == 'SUCCESS' else "❌"
            status_color = "#4CAF50" if r['status'] == 'SUCCESS' else "#f44336"
            
            file_details += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">{r['file_name']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{r['new_file_name']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{r['kecamatan']}</td>
                <td style="padding: 8px; border: 1px solid #ddd; color: {status_color}; font-weight: bold;">
                    {status_icon} {r['status']}
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{r['rows_processed']:,}</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">
                    {r['kecamatan_filled']:,} <br>
                    <small>({r['kecamatan_updated']:,} diperbarui)</small>
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">
                    {r['desa_filled']:,} <br>
                    <small>({r['desa_updated']:,} diperbarui)</small>
                </td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">
                    <span style="color: {'#4CAF50' if r['fill_percentage'] > 90 else '#ff9800' if r['fill_percentage'] > 50 else '#f44336'};">
                        {r['fill_percentage']:.1f}%
                    </span>
                </td>
            </tr>
            """
        
        # Buat email HTML
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <div style="max-width: 900px; margin: 0 auto; padding: 20px;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; text-align: center;">
                    <h1 style="margin: 0; font-size: 28px;">📊 LAPORAN UPDATE DATA KECAMATAN & DESA</h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9;">{datetime.now().strftime('%d %B %Y %H:%M:%S')}</p>
                    <p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.8;">📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py</p>
                </div>
                
                <div style="background: white; padding: 30px; border-radius: 0 0 10px 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                    
                    <!-- SUMMARY STATS -->
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px;">
                        <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #4CAF50;">
                            <div style="font-size: 32px; font-weight: bold; color: #2e7d32;">{total_files}</div>
                            <div style="color: #555;">Total File</div>
                        </div>
                        <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #4CAF50;">
                            <div style="font-size: 32px; font-weight: bold; color: #2e7d32;">{success_files}</div>
                            <div style="color: #555;">Berhasil</div>
                        </div>
                        <div style="background: #ffebee; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #f44336;">
                            <div style="font-size: 32px; font-weight: bold; color: #c62828;">{failed_files}</div>
                            <div style="color: #555;">Gagal</div>
                        </div>
                        <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #2196F3;">
                            <div style="font-size: 32px; font-weight: bold; color: #1565c0;">{total_rows:,}</div>
                            <div style="color: #555;">Total Baris</div>
                        </div>
                    </div>
                    
                    <!-- DETAILED TABLE -->
                    <h3 style="color: #333; border-bottom: 2px solid #eee; padding-bottom: 10px;">📋 Detail Proses File</h3>
                    <div style="overflow-x: auto;">
                        <table style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                            <thead>
                                <tr style="background-color: #4CAF50; color: white;">
                                    <th style="padding: 12px; text-align: left; border: 1px solid #ddd;">File Asal</th>
                                    <th style="padding: 12px; text-align: left; border: 1px solid #ddd;">File Baru</th>
                                    <th style="padding: 12px; text-align: left; border: 1px solid #ddd;">Kecamatan</th>
                                    <th style="padding: 12px; text-align: left; border: 1px solid #ddd;">Status</th>
                                    <th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Baris</th>
                                    <th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Kecamatan</th>
                                    <th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Desa</th>
                                    <th style="padding: 12px; text-align: center; border: 1px solid #ddd;">%</th>
                                </tr>
                            </thead>
                            <tbody>
                                {file_details}
                            </tbody>
                        </table>
                    </div>
                    
                    <!-- KEY ACHIEVEMENTS -->
                    <div style="background: #fff3cd; border: 1px solid #ffeaa7; border-left: 4px solid #f1c40f; padding: 15px; border-radius: 5px; margin-top: 25px;">
                        <h4 style="color: #856404; margin-top: 0;">🎯 Capaian Proses:</h4>
                        <ul style="color: #856404; margin-bottom: 0;">
                            <li><strong>Verifikasi Data:</strong> Mengecek dan memperbarui data yang sudah ada</li>
                            <li><strong>Perubahan Nama Kolom:</strong> Gapoktan → Kecamatan (jika diperlukan)</li>
                            <li><strong>Kolom Nama Desa:</strong> Ditambahkan jika belum ada</li>
                            <li><strong>Update Berulang:</strong> Data diperbarui meskipun file sudah memiliki nama yang benar</li>
                            <li><strong>Data Diperbarui:</strong> {total_kecamatan_updated:,} kecamatan & {total_desa_updated:,} desa diperbarui</li>
                            <li><strong>Rata-rata Pengisian:</strong> Kecamatan: {(total_kecamatan_filled/total_rows*100) if total_rows > 0 else 0:.1f}%, Desa: {(total_desa_filled/total_rows*100) if total_rows > 0 else 0:.1f}%</li>
                        </ul>
                    </div>
                    
                    <!-- SYSTEM INFO -->
                    <div style="background: #e8f5e9; border: 1px solid #c8e6c9; border-left: 4px solid #2E7D32; padding: 15px; border-radius: 5px; margin-top: 25px;">
                        <h4 style="color: #1B5E20; margin-top: 0;">⚙️ Informasi Sistem:</h4>
                        <ul style="color: #2E7D32; margin-bottom: 0;">
                            <li><strong>Repository:</strong> verval-pupuk2/scripts/nama_kecamatan_desa.py</li>
                            <li><strong>Folder Sumber:</strong> {FOLDER_ID}</li>
                            <li><strong>File Kode:</strong> {KODE_FILE_ID}</li>
                            <li><strong>Email Penerima:</strong> {len(EMAIL_CONFIG['recipient_emails'])} orang</li>
                            <li><strong>Waktu Proses:</strong> {datetime.now().strftime('%H:%M:%S')}</li>
                        </ul>
                    </div>
                    
                    <!-- FOOTER -->
                    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; text-align: center; color: #777; font-size: 12px;">
                        <p>Email ini dikirim otomatis oleh sistem update data kecamatan & desa ERDKK</p>
                        <p>📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py</p>
                        <p>© {datetime.now().year} - Sistem Pemantauan Data Pupuk</p>
                    </div>
                    
                </div>
            </div>
        </body>
        </html>
        """
        
        # Setup email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'[verval-pupuk2] Laporan Update Kecamatan & Desa - {datetime.now().strftime("%d/%m/%Y")}'
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ", ".join(EMAIL_CONFIG['recipient_emails'])
        
        msg.attach(MIMEText(html_content, 'html'))
        
        # Kirim email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.send_message(msg)
        
        print(f"📧 Laporan email terkirim ke {len(EMAIL_CONFIG['recipient_emails'])} penerima")
        return True
        
    except Exception as e:
        print(f"❌ Gagal mengirim email laporan: {str(e)}")
        return False

# ============================
# PROSES UTAMA
# ============================

def process_erdkk_files():
    """Proses utama untuk melengkapi dan memverifikasi data kecamatan dan desa"""
    results = []
    
    # Autentikasi
    drive_service = authenticate_drive()
    
    # Download file Kode Desa dan Kios
    print("📥 Mengunduh file Kode Desa dan Kios...")
    kode_file_path = download_file(drive_service, KODE_FILE_ID, 'kode_desa_kios.xlsx')
    
    # Baca data kode desa
    kode_df = pd.read_excel(kode_file_path)
    print(f"✅ Data kode desa berhasil dibaca: {len(kode_df)} baris")
    
    # Standarisasi nama kolom
    kode_df.columns = [str(col).strip() for col in kode_df.columns]
    
    # Mapping kolom (antisipasi penulisan berbeda)
    column_mapping = {
        'KECAMATAN': ['KECAMATAN', 'Kecamatan', 'kecamatan', 'NAMA KECAMATAN'],
        'Kode Desa': ['Kode Desa', 'Kode_Desa', 'KodeDesa', 'KODE DESA'],
        'Desa': ['Desa', 'DESA', 'NAMA DESA', 'Nama Desa']
    }
    
    # Normalisasi nama kolom
    for standard_name, variations in column_mapping.items():
        for variation in variations:
            if variation in kode_df.columns:
                kode_df = kode_df.rename(columns={variation: standard_name})
                break
    
    # Buat mapping kode desa ke kecamatan dan desa
    kode_to_kecamatan = {}
    kode_to_desa = {}
    
    for _, row in kode_df.iterrows():
        kode_desa = str(row['Kode Desa']).strip()
        kecamatan = str(row['KECAMATAN']).strip() if 'KECAMATAN' in kode_df.columns and pd.notna(row['KECAMATAN']) else ''
        desa = str(row['Desa']).strip() if 'Desa' in kode_df.columns and pd.notna(row['Desa']) else ''
        
        if kode_desa:
            if kecamatan:
                kode_to_kecamatan[kode_desa] = kecamatan
            if desa:
                kode_to_desa[kode_desa] = desa
    
    print(f"✅ Mapping berhasil dibuat:")
    print(f"  📍 Kode desa → kecamatan: {len(kode_to_kecamatan)} entri")
    print(f"  🏘️  Kode desa → desa: {len(kode_to_desa)} entri")
    
    # Dapatkan file-file ERDKK
    print(f"\n🔍 Mengambil daftar file dari folder...")
    erdkk_files = get_files_in_folder(drive_service, FOLDER_ID)
    
    if not erdkk_files:
        print("❌ Tidak ditemukan file Excel dalam folder")
        return results
    
    print(f"✅ Menemukan {len(erdkk_files)} file ERDKK")
    print("ℹ️ Proses akan berjalan meskipun data sudah ada atau file sudah memiliki nama yang benar.")
    
    # Proses setiap file
    for file in erdkk_files:
        file_id = file['id']
        original_name = file['name']
        print(f"\n{'='*50}")
        print(f"📂 Memproses file: {original_name}")
        print(f"{'='*50}")
        
        try:
            # Download file ERDKK
            temp_path = f'temp_{original_name}'
            erdkk_path = download_file(drive_service, file_id, temp_path)
            
            # Baca file ERDKK
            erdkk_df = pd.read_excel(erdkk_path)
            
            # Standarisasi kolom
            erdkk_df.columns = [str(col).strip() for col in erdkk_df.columns]
            
            # =============== ANALISIS AWAL ===============
            print("📊 Analisis kondisi file:")
            
            # Cek kolom Kecamatan (dengan berbagai variasi nama)
            kecamatan_col_found = False
            kecamatan_col_name = None
            for col in erdkk_df.columns:
                if 'kecamatan' in col.lower() or 'gapoktan' in col.lower():
                    kecamatan_col_found = True
                    kecamatan_col_name = col
                    break
            
            # Cek kolom Desa
            desa_col_found = False
            desa_col_name = None
            for col in erdkk_df.columns:
                if 'desa' in col.lower() and 'nama' in col.lower():
                    desa_col_found = True
                    desa_col_name = col
                    break
            
            print(f"  📍 Kolom Kecamatan ditemukan: {kecamatan_col_found} ({kecamatan_col_name})")
            print(f"  🏘️  Kolom Desa ditemukan: {desa_col_found} ({desa_col_name})")
            
            # =============== PROSES KOLOM KECAMATAN ===============
            # Jika tidak ada kolom Kecamatan, coba cari Gapoktan untuk diganti
            if not kecamatan_col_found:
                gapoktan_col_idx = -1
                gapoktan_col_name = None
                for idx, col in enumerate(erdkk_df.columns):
                    if 'gapoktan' in col.lower():
                        gapoktan_col_idx = idx
                        gapoktan_col_name = col
                        break
                
                if gapoktan_col_idx >= 0:
                    # Ganti nama kolom Gapoktan menjadi Kecamatan
                    erdkk_df = erdkk_df.rename(columns={gapoktan_col_name: 'Kecamatan'})
                    kecamatan_col_name = 'Kecamatan'
                    print(f"✅ Kolom '{gapoktan_col_name}' diganti menjadi 'Kecamatan'")
                else:
                    # Jika tidak ada kolom Gapoktan, tambahkan kolom Kecamatan
                    # Cari posisi yang tepat: setelah 'Nama Kios Pengecer'
                    nama_kios_idx = -1
                    for idx, col in enumerate(erdkk_df.columns):
                        if 'nama kios' in col.lower() or 'kios' in col.lower():
                            nama_kios_idx = idx
                            break
                    
                    if nama_kios_idx >= 0:
                        erdkk_df.insert(nama_kios_idx + 1, 'Kecamatan', np.nan)
                        kecamatan_col_name = 'Kecamatan'
                        print(f"✅ Kolom 'Kecamatan' ditambahkan setelah kolom {erdkk_df.columns[nama_kios_idx]}")
                    else:
                        # Default: tambahkan di posisi 4 (setelah beberapa kolom awal)
                        insert_pos = min(4, len(erdkk_df.columns))
                        erdkk_df.insert(insert_pos, 'Kecamatan', np.nan)
                        kecamatan_col_name = 'Kecamatan'
                        print(f"✅ Kolom 'Kecamatan' ditambahkan di posisi {insert_pos}")
            
            # =============== PROSES KOLOM DESA ===============
            # Jika tidak ada kolom Desa, tambahkan di akhir
            if not desa_col_found:
                erdkk_df['Nama Desa'] = np.nan
                desa_col_name = 'Nama Desa'
                print(f"✅ Kolom 'Nama Desa' ditambahkan di akhir file")
            
            # =============== ISI DATA KECAMATAN DAN DESA ===============
            kecamatan_data = []
            desa_data = []
            kecamatan_updated_count = 0
            desa_updated_count = 0
            kecamatan_found = None
            
            for idx, row in erdkk_df.iterrows():
                # 1. Dapatkan kode desa dari berbagai kemungkinan nama kolom
                kode_desa = ''
                kode_col_names = ['Kode Desa', 'Kode_Desa', 'KodeDesa', 'KODE DESA', 'KODES', 'Kode']
                
                for col_name in kode_col_names:
                    if col_name in erdkk_df.columns:
                        kode_val = row[col_name]
                        if pd.notna(kode_val):
                            kode_desa = str(kode_val).strip()
                            break
                
                # 2. Dapatkan data dari mapping
                kecamatan_from_mapping = kode_to_kecamatan.get(kode_desa, '')
                desa_from_mapping = kode_to_desa.get(kode_desa, '')
                
                # 3. Dapatkan data yang sudah ada di file
                current_kecamatan = str(row[kecamatan_col_name]).strip() if pd.notna(row[kecamatan_col_name]) else ''
                current_desa = str(row[desa_col_name]).strip() if pd.notna(row[desa_col_name]) else ''
                
                # 4. Tentukan nilai akhir (prioritas: data mapping > data existing)
                final_kecamatan = kecamatan_from_mapping if kecamatan_from_mapping else current_kecamatan
                final_desa = desa_from_mapping if desa_from_mapping else current_desa
                
                # 5. Hitung berapa data yang diperbarui
                if kecamatan_from_mapping and (not current_kecamatan or current_kecamatan != kecamatan_from_mapping):
                    kecamatan_updated_count += 1
                
                if desa_from_mapping and (not current_desa or current_desa != desa_from_mapping):
                    desa_updated_count += 1
                
                kecamatan_data.append(final_kecamatan)
                desa_data.append(final_desa)
                
                # 6. Catat kecamatan pertama yang ditemukan untuk rename file
                if final_kecamatan and kecamatan_found is None:
                    kecamatan_found = final_kecamatan
            
            # Update kolom Kecamatan dan Desa
            erdkk_df[kecamatan_col_name] = kecamatan_data
            erdkk_df[desa_col_name] = desa_data
            
            # Hitung statistik
            total_rows = len(erdkk_df)
            kecamatan_filled = sum(1 for x in kecamatan_data if x)
            desa_filled = sum(1 for x in desa_data if x)
            fill_percentage = ((kecamatan_filled + desa_filled) / (total_rows * 2) * 100) if total_rows > 0 else 0
            
            print(f"✅ Data berhasil diproses:")
            print(f"  📊 Total baris: {total_rows}")
            print(f"  📍 Kecamatan terisi: {kecamatan_filled} ({kecamatan_updated_count} diperbarui)")
            print(f"  🏘️  Desa terisi: {desa_filled} ({desa_updated_count} diperbarui)")
            print(f"  📈 Persentase pengisian: {fill_percentage:.1f}%")
            
            # =============== RENAME FILE ===============
            # Rename file berdasarkan kecamatan yang ditemukan
            new_filename = None
            should_rename = True
            
            # Cek apakah file sudah memiliki nama yang benar
            if kecamatan_found:
                clean_kecamatan = clean_filename(kecamatan_found)
                expected_filename = f"{clean_kecamatan}_ERDKK.xlsx"
                
                if original_name == expected_filename:
                    print(f"✅ File sudah memiliki nama yang benar: {original_name}")
                    new_filename = original_name
                    should_rename = False
                else:
                    new_filename = expected_filename
                    print(f"✅ File akan di-rename menjadi: {new_filename}")
            else:
                print("⚠️ Tidak ditemukan data kecamatan untuk rename file")
                new_filename = original_name
                should_rename = False
            
            # =============== SIMPAN DAN UPLOAD ===============
            # Simpan file yang telah diproses
            output_path = f'processed_{original_name}'
            erdkk_df.to_excel(output_path, index=False)
            
            # Update file yang sudah ada di Google Drive
            updated_file_id = update_file(drive_service, file_id, output_path)
            
            # Rename file jika diperlukan
            final_filename = new_filename
            if should_rename and kecamatan_found:
                final_filename = rename_file(drive_service, file_id, new_filename)
                print(f"✅ File berhasil di-rename menjadi: {final_filename}")
            
            # =============== SIMPAN HASIL ===============
            results.append({
                'file_name': original_name,
                'new_file_name': final_filename,
                'file_id': file_id,
                'kecamatan': kecamatan_found if kecamatan_found else 'Tidak Ditemukan',
                'status': 'SUCCESS',
                'rows_processed': total_rows,
                'kecamatan_filled': kecamatan_filled,
                'kecamatan_updated': kecamatan_updated_count,
                'desa_filled': desa_filled,
                'desa_updated': desa_updated_count,
                'fill_percentage': fill_percentage,
                'message': f'Berhasil memproses {total_rows} baris, {kecamatan_updated_count} kecamatan dan {desa_updated_count} desa diperbarui'
            })
            
            # Hapus file sementara
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if os.path.exists(output_path):
                os.remove(output_path)
            
        except Exception as e:
            print(f"❌ Error memproses {original_name}: {str(e)}")
            results.append({
                'file_name': original_name,
                'new_file_name': original_name,
                'file_id': file_id,
                'kecamatan': 'ERROR',
                'status': 'FAILED',
                'rows_processed': 0,
                'kecamatan_filled': 0,
                'kecamatan_updated': 0,
                'desa_filled': 0,
                'desa_updated': 0,
                'fill_percentage': 0,
                'message': str(e)
            })
    
    # Hapus file kode sementara
    if os.path.exists('kode_desa_kios.xlsx'):
        os.remove('kode_desa_kios.xlsx')
    
    return results

# ============================
# FUNGSI UTAMA
# ============================

def main():
    """Fungsi utama"""
    print("\n" + "="*60)
    print("🚀 SCRIPT UPDATE & VERIFIKASI DATA KECAMATAN & DESA ERDKK")
    print("="*60)
    print(f"📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py")
    print("="*60)
    
    try:
        # Jalankan proses utama
        print("\n🚀 Memulai proses update dan verifikasi data...")
        results = process_erdkk_files()
        
        # Tampilkan summary
        print("\n" + "="*60)
        print("📊 SUMMARY HASIL PROSES")
        print("="*60)
        
        total_success = sum(1 for r in results if r['status'] == 'SUCCESS')
        total_failed = len(results) - total_success
        total_rows = sum(r['rows_processed'] for r in results)
        total_kecamatan_filled = sum(r['kecamatan_filled'] for r in results)
        total_kecamatan_updated = sum(r['kecamatan_updated'] for r in results)
        total_desa_filled = sum(r['desa_filled'] for r in results)
        total_desa_updated = sum(r['desa_updated'] for r in results)
        
        kecamatan_percentage = (total_kecamatan_filled / total_rows * 100) if total_rows > 0 else 0
        desa_percentage = (total_desa_filled / total_rows * 100) if total_rows > 0 else 0
        
        print(f"\n📁 Total File: {len(results)}")
        print(f"✅ Berhasil: {total_success}")
        print(f"❌ Gagal: {total_failed}")
        print(f"📊 Total Baris: {total_rows:,}")
        print(f"📍 Kecamatan: {total_kecamatan_filled:,} terisi ({total_kecamatan_updated:,} diperbarui)")
        print(f"🏘️  Desa: {total_desa_filled:,} terisi ({total_desa_updated:,} diperbarui)")
        print(f"📈 Persentase: Kecamatan {kecamatan_percentage:.1f}%, Desa {desa_percentage:.1f}%")
        
        if total_success > 0:
            print("\n📋 Detail file yang berhasil:")
            for r in results:
                if r['status'] == 'SUCCESS':
                    rename_info = "" if r['file_name'] == r['new_file_name'] else f" → {r['new_file_name']}"
                    print(f"   • {r['file_name']}{rename_info}")
                    print(f"     📍 Kecamatan: {r['kecamatan']}")
                    print(f"     📊 Data: {r['kecamatan_filled']}/{r['rows_processed']} baris ({r['kecamatan_updated']} diperbarui)")
                    print(f"     🏘️  Desa: {r['desa_filled']}/{r['rows_processed']} baris ({r['desa_updated']} diperbarui)")
                    print()
        
        if total_failed > 0:
            print("\n⚠️ File yang gagal diproses:")
            for r in results:
                if r['status'] == 'FAILED':
                    print(f"   • {r['file_name']}: {r['message']}")
        
        # Kirim notifikasi email detail
        print("\n" + "-"*60)
        print("📧 Mengirim laporan email...")
        
        if results:
            if send_detailed_email_notification(results):
                print("✅ Laporan email berhasil dikirim")
            else:
                print("⚠️ Gagal mengirim laporan email, mengirim notifikasi sederhana...")
                # Kirim notifikasi sederhana sebagai fallback
                simple_message = f"""
PROSES UPDATE & VERIFIKASI DATA KECAMATAN & DESA SELESAI

📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
📁 Total File: {len(results)}
✅ Berhasil: {total_success}
❌ Gagal: {total_failed}
📊 Total Baris: {total_rows:,}
📍 Kecamatan: {total_kecamatan_filled:,} terisi ({total_kecamatan_updated:,} diperbarui)
🏘️  Desa: {total_desa_filled:,} terisi ({total_desa_updated:,} diperbarui)
📈 Persentase: Kecamatan {kecamatan_percentage:.1f}%, Desa {desa_percentage:.1f}%

PROSES YANG DILAKUKAN:
1. Verifikasi data yang sudah ada
2. Update data yang tidak sesuai dengan mapping
3. Tambah kolom jika belum ada
4. Rename file jika nama tidak sesuai
5. File dengan nama yang benar tetap diproses untuk verifikasi

Semua file telah diverifikasi dan diperbarui!
                """
                send_email_notification("PROSES UPDATE & VERIFIKASI ERDKK SELESAI", simple_message, is_success=True)
        else:
            send_email_notification("TIDAK ADA FILE UNTUK DIPROSES", 
                                  "Tidak ditemukan file ERDKK di folder Google Drive.", 
                                  is_success=False)
        
        print("\n" + "="*60)
        print("🎉 PROSES SELESAI!")
        print("="*60)
        
    except Exception as e:
        error_message = f"""
PROSES UPDATE & VERIFIKASI DATA KECAMATAN & DESA GAGAL

📁 Repository: verval-pupuk2/scripts/nama_kecamatan_desa.py
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
❌ Error: {str(e)}

🔧 Troubleshooting:
1. Pastikan secret 'GOOGLE_APPLICATION_CREDENTIALS_JSON' ada di GitHub Secrets
2. Pastikan service account memiliki akses ke folder dan file Google Drive
3. Pastikan FOLDER_ID dan KODE_FILE_ID benar
4. Pastikan ada file Excel di folder ERDKK
5. Pastikan file kode desa memiliki kolom 'Kode Desa', 'Kecamatan', dan 'Desa'
        """
        print(f"\n❌ Error dalam proses utama: {str(e)}")
        
        # Kirim email notifikasi error
        send_email_notification("PROSES UPDATE & VERIFIKASI ERDKK GAGAL", error_message, is_success=False)
        raise

# ============================
# JALANKAN SCRIPT
# ============================
if __name__ == "__main__":
    main()
