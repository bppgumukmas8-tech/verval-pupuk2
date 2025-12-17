#!/usr/bin/env python3
"""
erdkk_wa_center.py
ERDKK WA Center - Pivot Data Berdasarkan NIK/KTP Petani
Versi perbaikan dengan notifikasi email.

Lokasi: verval-pupuk2/scripts/erdkk_wa_center.py
"""

import os
import sys
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import io
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime
import json
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

# ==============================================
# KONFIGURASI
# ==============================================
FOLDER_ID = "1BBgVsgq7EMGs0TLaO_4GEtUppznm1v5J"
SPREADSHEET_ID = "1W5s0LPqugmcqsjPPaqzKlwUJBAmJfyls574eak_BQ5Q"

# ==============================================
# LOAD EMAIL CONFIGURATION FROM SECRETS
# ==============================================
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

# ==============================================
# FUNGSI UTILITAS
# ==============================================

def send_email_notification(subject, body, is_success=True):
    """Kirim notifikasi email TANPA attachment"""
    try:
        print(f"\n📧 Menyiapkan email notifikasi...")
        
        # Load config email
        EMAIL_CONFIG = load_email_config()
        
        # Setup email
        msg = MIMEMultipart()
        msg['Subject'] = f"[verval-pupuk2] {subject}"
        msg['From'] = EMAIL_CONFIG["sender_email"]
        msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
        
        # Ganti newline dengan <br> sebelum dimasukkan ke f-string
        body_html = body.replace('\n', '<br>')
        
        # Buat body email dengan format HTML
        if is_success:
            email_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                <div style="max-width: 800px; margin: 0 auto; padding: 20px; border: 2px solid #4CAF50; border-radius: 10px;">
                    <div style="background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0; text-align: center;">
                        <h1 style="margin: 0;">✅ {subject}</h1>
                        <p style="margin: 5px 0 0 0; opacity: 0.9;">{datetime.now().strftime('%d %B %Y %H:%M:%S')}</p>
                    </div>
                    
                    <div style="padding: 20px; background-color: #f9f9f9; border-radius: 0 0 8px 8px;">
                        <div style="background-color: white; padding: 15px; border-radius: 5px; border-left: 4px solid #4CAF50;">
                            {body_html}
                        </div>
                        
                        <div style="margin-top: 20px; padding: 15px; background-color: #e8f5e9; border-radius: 5px; border-left: 4px solid #2E7D32;">
                            <h3 style="color: #1B5E20; margin-top: 0;">📊 Informasi Sistem:</h3>
                            <ul style="color: #2E7D32;">
                                <li>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py</li>
                                <li>📁 Folder Sumber: {FOLDER_ID}</li>
                                <li>📊 Spreadsheet Tujuan: {SPREADSHEET_ID}</li>
                                <li>⏰ Waktu Proses: {datetime.now().strftime('%H:%M:%S')}</li>
                            </ul>
                        </div>
                        
                        <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px; border-top: 1px solid #eee; padding-top: 15px;">
                            <p>Email ini dikirim otomatis oleh sistem ERDKK WA Center</p>
                            <p>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py</p>
                            <p>© {datetime.now().year} - BPP Gumukmas</p>
                        </div>
                    </div>
                </div>
            </body>
            </html>
            """
        else:
            email_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                <div style="max-width: 800px; margin: 0 auto; padding: 20px; border: 2px solid #f44336; border-radius: 10px;">
                    <div style="background: linear-gradient(135deg, #f44336 0%, #d32f2f 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0; text-align: center;">
                        <h1 style="margin: 0;">❌ {subject}</h1>
                        <p style="margin: 5px 0 0 0; opacity: 0.9;">{datetime.now().strftime('%d %B %Y %H:%M:%S')}</p>
                    </div>
                    
                    <div style="padding: 20px; background-color: #fff5f5; border-radius: 0 0 8px 8px;">
                        <div style="background-color: white; padding: 15px; border-radius: 5px; border-left: 4px solid #f44336;">
                            {body_html}
                        </div>
                        
                        <div style="margin-top: 20px; padding: 15px; background-color: #ffebee; border-radius: 5px; border-left: 4px solid #c62828;">
                            <h3 style="color: #b71c1c; margin-top: 0;">⚠️ Troubleshooting:</h3>
                            <ul style="color: #c62828;">
                                <li>Periksa koneksi internet</li>
                                <li>Pastikan file Excel tersedia di Google Drive</li>
                                <li>Pastikan service account memiliki akses yang cukup</li>
                                <li>Periksa log error untuk detail lebih lanjut</li>
                            </ul>
                        </div>
                        
                        <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px; border-top: 1px solid #eee; padding-top: 15px;">
                            <p>Email ini dikirim otomatis oleh sistem ERDKK WA Center</p>
                            <p>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py</p>
                            <p>© {datetime.now().year} - BPP Gumukmas</p>
                        </div>
                    </div>
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
        
        print(f"   ✅ Email berhasil dikirim ke {len(EMAIL_CONFIG['recipient_emails'])} penerima")
        return True
        
    except Exception as e:
        print(f"   ❌ Gagal mengirim email: {e}")
        return False

def send_success_email(result_df, file_count, success_count, failed_count, backup_file=None):
    """Kirim email notifikasi sukses TANPA attachment"""
    try:
        total_petani = len(result_df)
        total_rows = result_df.shape[0]
        
        subject = f"ERDKK WA Center - Proses Pivot Data Selesai - {datetime.now().strftime('%d/%m/%Y')}"
        
        body = f"""
        📊 LAPORAN PROSES PIVOT DATA ERDKK
        
        ⏰ Waktu Proses: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
        📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py
        
        📈 HASIL STATISTIK:
        ──────────────────────────────
        📁 File Ditemukan: {file_count} file
        ✅ File Berhasil: {success_count} file
        ❌ File Gagal: {failed_count} file
        
        👥 DATA PETANI:
        ──────────────────────────────
        👤 Total Petani (NIK unik): {total_petani:,} petani
        📊 Total Baris Hasil: {total_rows:,} baris
        
        🔗 LINK HASIL:
        ──────────────────────────────
        Google Sheets: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
        
        📋 DETAIL PROSES:
        ──────────────────────────────
        1. Pengambilan file dari Google Drive
        2. Pembersihan dan validasi data NIK
        3. Pengambilan data desa dari kolom 'Nama Desa'
        4. Penggabungan data berdasarkan NIK
        5. Upload hasil ke Google Sheets
        6. Pengiriman notifikasi email
        
        ⚙️ KONFIGURASI:
        ──────────────────────────────
        • Folder Sumber: {FOLDER_ID}
        • Spreadsheet Tujuan: {SPREADSHEET_ID}
        • Penerima Email: {len(load_email_config()['recipient_emails'])} orang
        
        🎯 STATUS: PROSES BERHASIL DENGAN SEMPURNA
        """
        
        if backup_file:
            # Hanya sebutkan nama file, TIDAK attach
            body += f"\n💾 Backup File: {os.path.basename(backup_file)} (disimpan di server)"
        
        # Kirim email TANPA attachment
        email_sent = send_email_notification(subject, body, is_success=True)
        
        # HAPUS FILE BACKUP SETELAH EMAIL TERKIRIM
        if backup_file and os.path.exists(backup_file):
            try:
                os.remove(backup_file)
                print(f"   🗑️  File backup dihapus: {os.path.basename(backup_file)}")
            except Exception as e:
                print(f"   ⚠️ Gagal menghapus file backup: {e}")
        
        return email_sent
        
    except Exception as e:
        print(f"   ❌ Gagal membuat email sukses: {e}")
        return False

def send_error_email(error_message, file_count=0):
    """Kirim email notifikasi error"""
    subject = f"ERDKK WA Center - Proses Pivot Data Gagal - {datetime.now().strftime('%d/%m/%Y')}"
    
    body = f"""
    ❌ PROSES PIVOT DATA GAGAL
    
    ⏰ Waktu Error: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
    📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py
    
    📊 STATUS SEBELUM ERROR:
    ──────────────────────────────
    File yang diproses: {file_count} file
    
    🚨 ERROR DETAILS:
    ──────────────────────────────
    {error_message}
    
    🔧 TROUBLESHOOTING:
    ──────────────────────────────
    1. Periksa koneksi internet
    2. Pastikan file Excel tersedia di Google Drive
    3. Pastikan service account memiliki akses yang cukup
    4. Periksa format file Excel (harus .xlsx atau .xls)
    5. Pastikan kolom 'NIK' ada di semua file
    
    ⚙️ KONFIGURASI:
    ──────────────────────────────
    • Folder Sumber: {FOLDER_ID}
    • Spreadsheet Tujuan: {SPREADSHEET_ID}
    • Penerima Email: {len(load_email_config()['recipient_emails'])} orang
    
    📞 SUPPORT:
    ──────────────────────────────
    Hubungi administrator sistem untuk bantuan lebih lanjut.
    """
    
    return send_email_notification(subject, body, is_success=False)

def authenticate_google():
    """Autentikasi ke Google API dengan service account info di env var"""
    try:
        print("🔐 Memulai autentikasi Google...")
        creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not creds_json:
            print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan")
            return None

        credentials_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        print("✅ Autentikasi Google berhasil")
        return credentials
    except Exception as e:
        print(f"❌ Error autentikasi: {e}")
        return None

def extract_files_from_folder(folder_id, service):
    """Ekstrak file dari Google Drive"""
    try:
        print("🔍 Mencari file Excel di Google Drive...")

        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name, mimeType)",
            pageSize=200
        ).execute()

        all_files = results.get('files', [])

        if not all_files:
            print("❌ Tidak ada file di folder")
            return []

        # Filter file Excel
        excel_files = []
        for file in all_files:
            filename = file['name'].lower()
            if filename.endswith(('.xlsx', '.xls', '.xlsm')):
                excel_files.append(file)

        print(f"📂 Ditemukan {len(excel_files)} file Excel")
        for i, file in enumerate(excel_files, 1):
            print(f"   {i:2d}. {file['name']}")

        return excel_files

    except Exception as e:
        print(f"❌ Error mengakses Google Drive: {e}")
        return []

def find_column_by_keywords(df, keywords, exact_match=False):
    """Cari kolom berdasarkan keywords (kembalikan nama kolom atau None)"""
    for col in df.columns:
        col_str = str(col).strip()
        col_lower = col_str.lower()
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            
            if exact_match:
                # Pencarian tepat (case-insensitive)
                if col_lower == keyword_lower:
                    return col
            else:
                # Pencarian mengandung keyword
                if keyword_lower in col_lower:
                    return col
    return None

def clean_and_convert_numeric(value):
    """Bersihkan dan konversi nilai numerik"""
    if pd.isna(value) or value == '':
        return 0.0
    try:
        value_str = str(value)
        value_str = re.sub(r'[^\d.,-]', '', value_str)
        value_str = value_str.replace(',', '.')
        # Hapus multiple titik kecuali terakhir
        if value_str.count('.') > 1:
            parts = value_str.split('.')
            value_str = ''.join(parts[:-1]) + '.' + parts[-1]
        return float(value_str)
    except:
        return 0.0

def extract_luas_column(df, keywords, mt_number=None):
    """
    Cari kolom luas berdasarkan keywords dan musim tanam tertentu
    Keywords: ['luas tanam', 'luas lahan', 'luas']
    mt_number: 1, 2, atau 3
    """
    for col in df.columns:
        col_lower = str(col).lower()
        has_keyword = any(k in col_lower for k in keywords)
        if not has_keyword:
            continue
        
        if mt_number is None:
            # Jika tidak mencari musim tertentu, ambil yang pertama cocok
            return col
        
        # Cari berdasarkan musim tanam
        mt_patterns = [
            f'mt{mt_number}',
            f'mt {mt_number}',
            f'musim {mt_number}',
            f'mt {mt_number}',
            f'mt{mt_number}'
        ]
        if any(pattern in col_lower for pattern in mt_patterns):
            return col
    
    return None

def read_and_process_excel(file_id, drive_service, filename):
    """Baca dan proses file Excel dengan deteksi kolom yang ditingkatkan"""
    try:
        print(f"\n📖 Memproses: {filename}")
        
        # Download file
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        while True:
            status, done = downloader.next_chunk()
            if done:
                break

        file_content = fh.getvalue()

        # Baca file Excel
        try:
            df = pd.read_excel(io.BytesIO(file_content), dtype=str, na_filter=False)
        except Exception as e:
            print(f"   ⚠️ Error membaca: {e}")
            return None

        print(f"   📊 Data mentah: {len(df)} baris, {len(df.columns)} kolom")

        # DETEKSI KOLOM
        # 1. NIK
        nik_col = find_column_by_keywords(df, ['nik', 'ktp', 'no. ktp', 'noktp', 'no ktp'])
        if not nik_col:
            print(f"   ❌ Kolom NIK tidak ditemukan")
            return None
        print(f"   ✅ Kolom NIK: '{nik_col}'")

        # 2. Nama Petani
        nama_col = find_column_by_keywords(df, ['nama petani', 'nama_petani', 'nama petani'])
        if not nama_col:
            for col in df.columns:
                col_lower = str(col).lower()
                if 'nama' in col_lower and 'penyuluh' not in col_lower:
                    nama_col = col
                    break
        if not nama_col:
            nama_col = find_column_by_keywords(df, ['nama'])
        if nama_col:
            print(f"   ✅ Kolom Nama Petani: '{nama_col}'")
        else:
            print(f"   ⚠️ Kolom Nama Petani tidak ditemukan")

        # 3. Poktan
        poktan_col = find_column_by_keywords(df, ['nama poktan', 'poktan', 'kelompok tani', 'poktan'])
        if poktan_col:
            print(f"   ✅ Kolom Poktan: '{poktan_col}'")
        else:
            print(f"   ⚠️ Kolom Poktan tidak ditemukan")

        # 4. Nama Desa - HANYA ambil kolom dengan 'Nama Desa'
        desa_col = None
        
        # Prioritas 1: Kolom dengan nama persis 'Nama Desa' (case-insensitive)
        desa_col = find_column_by_keywords(df, ['Nama Desa'], exact_match=False)
        
        # Prioritas 2: Kolom yang mengandung 'nama desa' (bukan 'kode desa')
        if not desa_col:
            for col in df.columns:
                col_lower = str(col).lower()
                if 'nama desa' in col_lower and 'kode' not in col_lower:
                    desa_col = col
                    break
        
        if desa_col:
            print(f"   ✅ Kolom Nama Desa: '{desa_col}'")
        else:
            print(f"   ⚠️ Kolom Nama Desa tidak ditemukan")

        # 5. Nama Kios
        kios_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if 'nama kios pengecer' in col_lower:
                kios_col = col
                print(f"   ✅ Kolom Nama Kios: '{kios_col}'")
                break
        
        if not kios_col:
            print(f"   ⚠️ Kolom Nama Kios tidak ditemukan")

        # 6. Komoditas
        komoditas_cols = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if any(k in col_lower for k in ['komoditas', 'komoditi', 'jenis']):
                if 'mt1' in col_lower or 'musim 1' in col_lower or 'mt 1' in col_lower:
                    komoditas_cols['mt1'] = col
                elif 'mt2' in col_lower or 'musim 2' in col_lower or 'mt 2' in col_lower:
                    komoditas_cols['mt2'] = col
                elif 'mt3' in col_lower or 'musim 3' in col_lower or 'mt 3' in col_lower:
                    komoditas_cols['mt3'] = col
                elif 'komoditas' in col_lower:
                    komoditas_cols['general'] = col
        
        print(f"   ✅ Kolom Komoditas ditemukan: {len(komoditas_cols)}")

        # 7. Luas Tanam
        luas_cols = {}
        luas_keywords = ['luas tanam', 'luas lahan', 'luas']
        
        for mt in [1, 2, 3]:
            col = extract_luas_column(df, luas_keywords, mt)
            if col:
                luas_cols[f'mt{mt}'] = col
                print(f"   ✅ Kolom Luas MT{mt}: '{col}'")
        
        if not luas_cols:
            print(f"   ⚠️ Kolom Luas tidak ditemukan")

        # 8. Kolom Pupuk
        pupuk_columns = {}
        for col in df.columns:
            col_lower = str(col).lower()
            # MT1
            if 'urea' in col_lower and ('mt1' in col_lower or 'musim 1' in col_lower or 'mt 1' in col_lower):
                pupuk_columns['urea_mt1'] = col
            elif 'npk formula' in col_lower and ('mt1' in col_lower or 'musim 1' in col_lower):
                pupuk_columns['npk_formula_mt1'] = col
            elif 'npk' in col_lower and ('mt1' in col_lower or 'musim 1' in col_lower or 'mt 1' in col_lower) and 'formula' not in col_lower:
                pupuk_columns['npk_mt1'] = col
            elif 'organik' in col_lower and ('mt1' in col_lower or 'musim 1' in col_lower):
                pupuk_columns['organik_mt1'] = col
            # MT2
            elif 'urea' in col_lower and ('mt2' in col_lower or 'musim 2' in col_lower or 'mt 2' in col_lower):
                pupuk_columns['urea_mt2'] = col
            elif 'npk formula' in col_lower and ('mt2' in col_lower or 'musim 2' in col_lower):
                pupuk_columns['npk_formula_mt2'] = col
            elif 'npk' in col_lower and ('mt2' in col_lower or 'musim 2' in col_lower or 'mt 2' in col_lower) and 'formula' not in col_lower:
                pupuk_columns['npk_mt2'] = col
            elif 'organik' in col_lower and ('mt2' in col_lower or 'musim 2' in col_lower):
                pupuk_columns['organik_mt2'] = col
            # MT3
            elif 'urea' in col_lower and ('mt3' in col_lower or 'musim 3' in col_lower or 'mt 3' in col_lower):
                pupuk_columns['urea_mt3'] = col
            elif 'npk formula' in col_lower and ('mt3' in col_lower or 'musim 3' in col_lower):
                pupuk_columns['npk_formula_mt3'] = col
            elif 'npk' in col_lower and ('mt3' in col_lower or 'musim 3' in col_lower or 'mt 3' in col_lower) and 'formula' not in col_lower:
                pupuk_columns['npk_mt3'] = col
            elif 'organik' in col_lower and ('mt3' in col_lower or 'musim 3' in col_lower):
                pupuk_columns['organik_mt3'] = col

        print(f"   🌾 Kolom Pupuk yang ditemukan: {len(pupuk_columns)}")

        # BERSIHKAN DATA
        clean_df = pd.DataFrame()
        
        # NIK
        clean_df['nik'] = df[nik_col].astype(str).str.strip().apply(lambda x: re.sub(r'\D', '', x))
        clean_df = clean_df[clean_df['nik'].str.len() >= 10].copy()
        if clean_df.empty:
            print(f"   ⚠️ Tidak ada NIK valid setelah cleaning")
            return None

        idxs = clean_df.index

        # Nama Petani
        if nama_col and nama_col in df.columns:
            clean_df['nama_petani'] = df.loc[idxs, nama_col].astype(str).str.strip()
        else:
            clean_df['nama_petani'] = ''

        # Poktan
        if poktan_col and poktan_col in df.columns:
            clean_df['poktan'] = df.loc[idxs, poktan_col].astype(str).str.strip()
            clean_df['poktan'] = clean_df['poktan'].replace(['', 'nan', 'NaN', 'Nan', 'NA', 'N/A', '-'], 'Tidak disebutkan')
        else:
            clean_df['poktan'] = 'Tidak disebutkan'

        # Desa
        if desa_col and desa_col in df.columns:
            desa_values = df.loc[idxs, desa_col].astype(str).str.strip()
            desa_values = desa_values.replace(['', 'nan', 'NaN', 'Nan', 'NA', 'N/A', '-'], 'Desa tidak diketahui')
            clean_df['desa'] = desa_values
        else:
            clean_df['desa'] = 'Desa tidak diketahui'

        # Kecamatan
        kec_col = find_column_by_keywords(df, ['kecamatan', 'nama kecamatan'])
        if kec_col and kec_col in df.columns:
            clean_df['kecamatan'] = df.loc[idxs, kec_col].astype(str).str.strip()
        else:
            kec_name = filename.replace('_ERDKK.xlsx', '').replace('.xlsx', '').replace('.xls', '')
            clean_df['kecamatan'] = kec_name

        # Nama Kios
        if kios_col and kios_col in df.columns:
            clean_df['kios'] = df.loc[idxs, kios_col].astype(str).str.strip()
            clean_df['kios'] = clean_df['kios'].replace(['', 'nan', 'NaN', 'Nan'], 'Tidak disebutkan')
        else:
            clean_df['kios'] = 'Tidak disebutkan'

        # Komoditas
        komoditas_list = []
        for mt in ['mt1', 'mt2', 'mt3', 'general']:
            if mt in komoditas_cols and komoditas_cols[mt] in df.columns:
                kom_values = df.loc[idxs, komoditas_cols[mt]].astype(str).str.strip()
                komoditas_list.append(kom_values)
        
        if komoditas_list:
            all_komoditas = pd.Series([''] * len(idxs), index=idxs)
            for kom_series in komoditas_list:
                all_komoditas = all_komoditas + kom_series + ','
            clean_df['komoditas_raw'] = all_komoditas.str.rstrip(',').replace(['', ',', 'nan', 'NaN'], '')
        else:
            clean_df['komoditas_raw'] = ''

        # Luas Tanam
        luas_total = pd.Series([0.0] * len(idxs), index=idxs)
        
        for mt in ['mt1', 'mt2', 'mt3']:
            if mt in luas_cols and luas_cols[mt] in df.columns:
                luas_values = df.loc[idxs, luas_cols[mt]].apply(clean_and_convert_numeric)
                luas_total = luas_total + luas_values
        
        clean_df['luas_tanam'] = luas_total

        # Pupuk fields
        pupuk_keys = ['urea_mt1', 'npk_mt1', 'npk_formula_mt1', 'organik_mt1',
                     'urea_mt2', 'npk_mt2', 'npk_formula_mt2', 'organik_mt2',
                     'urea_mt3', 'npk_mt3', 'npk_formula_mt3', 'organik_mt3']
        for key in pupuk_keys:
            if key in pupuk_columns and pupuk_columns[key] in df.columns:
                clean_df[key] = df.loc[idxs, pupuk_columns[key]].apply(clean_and_convert_numeric)
            else:
                clean_df[key] = 0.0

        return clean_df

    except Exception as e:
        print(f"   ❌ Error memproses file: {e}")
        return None

def choose_nama_from_group(group):
    """Pilih nama petani yang paling mungkin benar"""
    names = group['nama_petani'].astype(str).str.strip()
    candidates = names[(names != '') & (~names.str.lower().str.contains('penyuluh'))]
    if not candidates.empty:
        mode = candidates.mode()
        if not mode.empty:
            return mode.iloc[0]
        else:
            return candidates.iloc[0]
    non_empty = names[names != '']
    if not non_empty.empty:
        return non_empty.iloc[non_empty.str.len().argmax()]
    return ''

def format_poktan_details_row(row):
    """Format detail satu baris poktan menjadi teks"""
    parts = []
    
    poktan = str(row.get('poktan', 'Tidak disebutkan')).strip()
    desa = str(row.get('desa', '')).strip()
    kec = str(row.get('kecamatan', '')).strip()
    luas = float(row.get('luas_tanam', 0.0))
    
    if poktan and poktan.lower() not in ['nan', 'tidak disebutkan', '']:
        parts.append(f"Poktan {poktan} Desa {desa} Kec. {kec},")
    else:
        parts.append(f"Poktan (tidak disebutkan) Desa {desa} Kec. {kec},")
    
    parts.append(f"Luas Tanam setahun {luas:.2f} Ha,")
    
    # Pupuk
    urea_mt1 = float(row.get('urea_mt1', 0))
    npk_mt1 = float(row.get('npk_mt1', 0))
    npk_formula_mt1 = float(row.get('npk_formula_mt1', 0))
    organik_mt1 = float(row.get('organik_mt1', 0))
    
    urea_mt2 = float(row.get('urea_mt2', 0))
    npk_mt2 = float(row.get('npk_mt2', 0))
    npk_formula_mt2 = float(row.get('npk_formula_mt2', 0))
    organik_mt2 = float(row.get('organik_mt2', 0))
    
    urea_mt3 = float(row.get('urea_mt3', 0))
    npk_mt3 = float(row.get('npk_mt3', 0))
    npk_formula_mt3 = float(row.get('npk_formula_mt3', 0))
    organik_mt3 = float(row.get('organik_mt3', 0))
    
    if urea_mt1 > 0 or npk_mt1 > 0 or npk_formula_mt1 > 0 or organik_mt1 > 0:
        mt1 = f"*. Urea MT1 {urea_mt1:.0f} kg, NPK MT1 {npk_mt1:.0f} kg, NPK Formula MT1 {npk_formula_mt1:.0f} kg, Organik MT1 {organik_mt1:.0f} kg,"
        parts.append(mt1)
    
    if urea_mt2 > 0 or npk_mt2 > 0 or npk_formula_mt2 > 0 or organik_mt2 > 0:
        mt2 = f"*. Urea MT2 {urea_mt2:.0f} kg, NPK MT2 {npk_mt2:.0f} kg, NPK Formula MT2 {npk_formula_mt2:.0f} kg, Organik MT2 {organik_mt2:.0f} kg,"
        parts.append(mt2)
    
    if urea_mt3 > 0 or npk_mt3 > 0 or npk_formula_mt3 > 0 or organik_mt3 > 0:
        mt3 = f"*. Urea MT3 {urea_mt3:.0f} kg, NPK MT3 {npk_mt3:.0f} kg, NPK Formula MT3 {npk_formula_mt3:.0f} kg, Organik MT3 {organik_mt3:.0f} kg,"
        parts.append(mt3)
    
    # Kios
    kios = str(row.get('kios', '')).strip()
    if kios and kios.lower() not in ['nan', 'tidak disebutkan', '']:
        parts.append(f'Kios layanan {kios}, Desa {desa}')
    
    # Komoditas
    kom = str(row.get('komoditas_raw', '')).strip()
    if kom and kom.lower() not in ['nan', '']:
        kom_list = re.split(r'[;,/]+', kom)
        kom_unique = []
        for k in kom_list:
            k_clean = k.strip()
            if k_clean and k_clean.lower() not in [x.lower() for x in kom_unique]:
                kom_unique.append(k_clean)
        
        if kom_unique:
            parts.append(f"Komoditas {', '.join(kom_unique)}")
    
    return "\n".join(parts)

def pivot_and_format_data(df_list):
    """Pivot dan format data; hasil hanya 3 kolom: nik, nama_petani, data"""
    if not df_list:
        return pd.DataFrame(columns=['nik','nama_petani','data'])

    print("\n" + "="*60)
    print("🔄 MENGGABUNGKAN SEMUA DATA")
    print("="*60)

    all_data = pd.concat(df_list, ignore_index=True, sort=False)
    print(f"📊 Total data gabungan: {len(all_data):,} baris")
    print(f"🔢 NIK unik: {all_data['nik'].nunique():,}")

    result_rows = []
    grouped = all_data.groupby('nik', sort=False)
    total_groups = len(grouped)

    for i, (nik, group) in enumerate(grouped, 1):
        if i % 50 == 0 or i == 1 or i == total_groups:
            print(f"   Memproses NIK ke-{i:,}/{total_groups:,}...")

        nama_petani = choose_nama_from_group(group)

        # buat detail per poktan
        poktan_details = []
        for _, row in group.iterrows():
            rowd = row.to_dict()
            poktan_details.append(format_poktan_details_row(rowd))

        # gabungkan dengan penomoran
        formatted_poktans = [f"{j+1}. {d}" for j,d in enumerate(poktan_details)]

        if poktan_details:
            if len(poktan_details) == 1:
                data_field = f"Nama {nama_petani} terdaftar di:\n    {formatted_poktans[0]}"
            else:
                data_field = f"Nama {nama_petani} terdaftar di:\n    " + "\n\n".join(formatted_poktans)
        else:
            data_field = f"Nama {nama_petani} terdaftar di: (tidak ada data)"

        result_rows.append({
            'nik': nik,
            'nama_petani': nama_petani,
            'data': data_field
        })

    result_df = pd.DataFrame(result_rows, columns=['nik','nama_petani','data'])

    print("\n" + "="*60)
    print("✅ PIVOT SELESAI")
    print("="*60)
    print(f"📈 Statistik:")
    print(f"   👤 Petani unik (baris hasil): {len(result_df):,}")
    print("="*60)

    return result_df

def upload_to_google_sheets(df, spreadsheet_id, credentials):
    """Upload ke Google Sheets dengan chunking untuk menghindari timeout"""
    try:
        print("\n📤 MENGUPLOAD KE GOOGLE SHEETS...")
        print(f"   📊 Data size: {len(df)} rows, {len(df.columns)} columns")
        
        sheets_service = build('sheets', 'v4', credentials=credentials)

        # Clear sheet terlebih dahulu
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:Z"
        ).execute()
        
        print("   ✅ Sheet cleared")

        # CHUNKING: Upload data dalam batch
        batch_size = 5000  # Google Sheets max sekitar 5000 baris per batch
        total_rows = len(df)
        
        if total_rows <= batch_size:
            # Jika data kecil, upload sekaligus
            headers = df.columns.tolist()
            values = df.fillna('').values.tolist()
            data = [headers] + values
            
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A1",
                valueInputOption="USER_ENTERED",
                body={"values": data}
            ).execute()
            
            print(f"   ✅ Berhasil mengupload {total_rows} baris sekaligus")
        else:
            # Jika data besar, upload per batch
            print(f"   📦 Data besar: {total_rows} baris, akan di-upload per {batch_size} baris")
            
            # Upload header dulu
            headers = df.columns.tolist()
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            print("   ✅ Header uploaded")
            
            # Upload data per batch
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                batch_values = batch_df.fillna('').values.tolist()
                
                # Range untuk batch ini (baris mulai dari 2 karena header di row 1)
                range_start = start_idx + 2  # +2 karena row 1 adalah header
                range_end = end_idx + 1
                range_name = f"Sheet1!A{range_start}:Z{range_end}"
                
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption="USER_ENTERED",
                    body={"values": batch_values}
                ).execute()
                
                print(f"   📥 Batch {start_idx//batch_size + 1}: rows {start_idx+1}-{end_idx} uploaded")
                
                # Tunggu sebentar antara batch untuk menghindari rate limit
                if end_idx < total_rows:
                    import time
                    time.sleep(1)
            
            print(f"   ✅ Total {total_rows} baris berhasil di-upload dalam {((total_rows-1)//batch_size)+1} batch")
        
        print(f"🔗 Link: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        return True

    except Exception as e:
        print(f"❌ Error upload: {e}")
        print(f"   🔧 Detail error: {type(e).__name__}")
        return False

def save_backup(df):
    """Simpan backup lokal"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ERDKK_Hasil_{timestamp}.csv"

        df.to_csv(filename, index=False, encoding='utf-8-sig')

        print(f"💾 Backup disimpan: {filename}")
        print(f"   📁 Ukuran: {os.path.getsize(filename) / 1024:.1f} KB")

        return filename
    except Exception as e:
        print(f"⚠️ Gagal menyimpan backup: {e}")
        return None

def cleanup_temp_files():
    """Hapus semua file temporary"""
    import glob
    temp_patterns = ['temp_*.xlsx', 'temp_*.xls', 'processed_*.xlsx', 'processed_*.xls', 'ERDKK_Hasil_*.csv']
    
    for pattern in temp_patterns:
        for file in glob.glob(pattern):
            try:
                os.remove(file)
                print(f"🗑️  File temporary dihapus: {file}")
            except:
                pass

# ==============================================
# FUNGSI UTAMA
# ==============================================

def main():
    """Fungsi utama"""
    print("\n" + "="*60)
    print("🚀 ERDKK WA CENTER - SISTEM PIVOT DATA")
    print("="*60)
    print(f"📁 Repository: verval-pupuk2/scripts/erdkk_wa_center.py")
    print(f"📅 Waktu mulai: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📁 Folder sumber: {FOLDER_ID}")
    print(f"📊 Sheet tujuan: {SPREADSHEET_ID}")
    print("="*60)

    backup_file = None  # Variabel untuk menyimpan nama file backup
    
    try:
        # Kirim email notifikasi mulai
        send_email_notification(
            "ERDKK WA Center - Proses Pivot Data Dimulai",
            f"""
📅 Proses pivot data ERDKK telah dimulai pada {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}.

📁 Informasi Sistem:
• Repository: verval-pupuk2/scripts/erdkk_wa_center.py
• Folder Sumber: {FOLDER_ID}
• Spreadsheet Tujuan: {SPREADSHEET_ID}

⏳ Sistem sedang memproses data dari folder Google Drive...
            """,
            is_success=True
        )

        # Autentikasi
        print("\n🔐 AUTENTIKASI GOOGLE...")
        credentials = authenticate_google()
        if not credentials:
            error_msg = "Gagal autentikasi ke Google API"
            print(f"❌ {error_msg}")
            send_error_email(error_msg)
            sys.exit(1)

        drive_service = build('drive', 'v3', credentials=credentials)

        # Ambil file
        print("\n🔍 MENGAMBIL FILE DARI GOOGLE DRIVE...")
        files = extract_files_from_folder(FOLDER_ID, drive_service)
        if not files:
            error_msg = "Tidak ditemukan file Excel di folder Google Drive"
            print(f"❌ {error_msg}")
            send_error_email(error_msg)
            sys.exit(1)

        # Proses setiap file
        all_data = []
        berhasil = 0
        gagal = 0

        print(f"\n📁 MEMPROSES {len(files)} FILE...")
        for i, file in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}] ", end="")
            df = read_and_process_excel(file['id'], drive_service, file['name'])

            if df is not None and not df.empty:
                all_data.append(df)
                berhasil += 1
                print(f"   ✅ Berhasil ({len(df)} baris)")
            else:
                gagal += 1
                print(f"   ❌ Gagal")

        print(f"\n📊 RINGKASAN PEMROSESAN:")
        print(f"   ✅ Berhasil: {berhasil} file")
        print(f"   ❌ Gagal: {gagal} file")

        if not all_data:
            error_msg = "Tidak ada data valid yang dapat diproses"
            print(f"❌ {error_msg}")
            send_error_email(error_msg, len(files))
            sys.exit(1)

        # Pivot
        print("\n" + "="*60)
        print("🔄 MEMBUAT PIVOT DATA...")
        result_df = pivot_and_format_data(all_data)
        if result_df.empty:
            error_msg = "Tidak ada data hasil pivot"
            print(f"❌ {error_msg}")
            send_error_email(error_msg, len(files))
            sys.exit(1)

        # Upload
        print("\n☁️ MENGUPLOAD HASIL...")
        upload_success = upload_to_google_sheets(result_df, SPREADSHEET_ID, credentials)
        
        # Simpan backup (selalu lakukan ini)
        backup_file = save_backup(result_df)
        
        if not upload_success:
            print("\n⚠️ Upload ke Google Sheets gagal, tetapi proses akan dilanjutkan")
            print("   📧 Email akan dikirim tanpa attachment")

        # Kirim email notifikasi TANPA attachment
        print("\n📧 MENGIRIM NOTIFIKASI EMAIL...")
        if send_success_email(result_df, len(files), berhasil, gagal, backup_file):
            print("✅ Email notifikasi berhasil dikirim")
        else:
            print("⚠️ Gagal mengirim email notifikasi, tetapi proses tetap berhasil")
        
        if not upload_success:
            print("\n❌ PROSES TIDAK SEMPURNA: Upload ke Google Sheets gagal")
            print("   ℹ️  Data tersedia di file backup dan link Google Sheets")
            sys.exit(1)

        print("\n" + "="*60)
        print("🎉 PROSES SELESAI DENGAN SUKSES!")
        print("="*60)
        print(f"📊 HASIL AKHIR:")
        print(f"   👥 Jumlah petani: {len(result_df):,}")
        if backup_file and os.path.exists(backup_file):
            print(f"   💾 File backup dibuat: {backup_file}")
        print(f"   🔗 Google Sheets: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
        print("="*60)

    except Exception as e:
        error_msg = f"Error dalam proses utama: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        print(f"\n❌ {error_msg}")
        send_error_email(error_msg)
        sys.exit(1)
    
    finally:
        # CLEANUP: Hapus file backup jika ada
        if backup_file and os.path.exists(backup_file):
            try:
                os.remove(backup_file)
                print(f"\n🗑️  File backup dihapus: {backup_file}")
            except Exception as e:
                print(f"⚠️ Gagal menghapus file backup: {e}")
        
        # Juga hapus file temporary lainnya jika ada
        cleanup_temp_files()

if __name__ == "__main__":
    main()
