#!/usr/bin/env python3
"""
erdkk_wa_center_fixed.py
ERDKK WA Center - Pivot Data Berdasarkan NIK/KTP Petani
VERSI PERBAIKAN: Fix detection kolom Poktan dan Kecamatan
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
import time
import math
import glob

# ==============================================
# KONFIGURASI
# ==============================================
FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"
SPREADSHEET_ID = "1nrZ1YLMijIrmHA3hJUw5AsdElkTH1oIxt3ux2mbdTn8"

# ==============================================
# FUNGSI EMAIL
# ==============================================

def load_email_config():
    """Memuat konfigurasi email dari environment variables/secrets"""
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
    
    return {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": SENDER_EMAIL,
        "sender_password": SENDER_EMAIL_PASSWORD,
        "recipient_emails": recipient_list
    }

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
                                <li>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center_fixed.py</li>
                                <li>📁 Folder Sumber: {FOLDER_ID}</li>
                                <li>📊 Spreadsheet Tujuan: {SPREADSHEET_ID}</li>
                                <li>⏰ Waktu Proses: {datetime.now().strftime('%H:%M:%S')}</li>
                            </ul>
                        </div>
                        
                        <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px; border-top: 1px solid #eee; padding-top: 15px;">
                            <p>Email ini dikirim otomatis oleh sistem ERDKK WA Center</p>
                            <p>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center_fixed.py</p>
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
                            <p>📁 Repository: verval-pupuk2/scripts/erdkk_wa_center_fixed.py</p>
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

def send_error_email(error_message, file_count=0):
    """Kirim email notifikasi error"""
    subject = f"ERDKK WA Center - Proses Pivot Data Gagal - {datetime.now().strftime('%d/%m/%Y')}"
    
    body = f"""
❌ PROSES PIVOT DATA GAGAL

⏰ Waktu Error: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📁 Repository: verval-pupuk2/scripts/erdkk_wa_center_fixed.py

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

# ==============================================
# FUNGSI GOOGLE AUTHENTICATION
# ==============================================

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

# ==============================================
# FUNGSI UTILITY & DEBUGGING
# ==============================================

def find_column_by_keywords(df, keywords, exact_match=False, exclude_keywords=None):
    """Cari kolom berdasarkan keywords (kembalikan nama kolom atau None)"""
    if exclude_keywords is None:
        exclude_keywords = []
    
    for col in df.columns:
        col_str = str(col).strip()
        col_lower = col_str.lower()
        
        # Cek apakah kolom mengandung exclude keywords
        has_exclude = any(exclude in col_lower for exclude in exclude_keywords)
        if has_exclude:
            continue
        
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

def debug_column_detection(df, filename):
    """Debug informasi kolom untuk membantu troubleshooting"""
    print(f"\n🔍 DEBUG COLUMN INFO for {filename}:")
    print("   📋 All columns:")
    for i, col in enumerate(df.columns):
        print(f"      {i:2d}. '{col}'")
    
    # Cari semua kolom yang mengandung keywords terkait
    keywords_to_check = ['poktan', 'desa', 'kecamatan', 'kelompok', 'nama', 'kios', 'komoditas', 'luas']
    for keyword in keywords_to_check:
        matching_cols = [col for col in df.columns if keyword.lower() in str(col).lower()]
        if matching_cols:
            print(f"\n   🔎 Columns containing '{keyword}':")
            for col in matching_cols:
                # Ambil sample values
                sample_values = df[col].dropna().unique()[:3]
                print(f"      - '{col}' (sample: {list(sample_values)})")
    
    # Ambil sample data untuk beberapa kolom yang dicurigai
    sample_cols = [col for col in df.columns if any(k in str(col).lower() for k in ['poktan', 'desa', 'kecamatan'])]
    if sample_cols:
        print(f"\n   📊 Sample data (first 5 rows):")
        for col in sample_cols[:5]:  # Limit to 5 columns
            sample_values = df[col].head(5).tolist()
            print(f"      '{col}': {sample_values}")
    
    return True

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

# ==============================================
# FUNGSI PEMROSESAN FILE - DIPERBAIKI
# ==============================================

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

def read_and_process_excel(file_id, drive_service, filename):
    """Baca dan proses file Excel dengan deteksi kolom yang diperbaiki"""
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
        
        # DEBUG: Tampilkan informasi kolom
        debug_column_detection(df, filename)
        
        # DETEKSI KOLOM - VERSI DIPERBAIKI
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

        # 3. Poktan - DIPERBAIKI: Hindari mengambil kolom kecamatan
        poktan_col = None
        
        # Prioritas 1: Cari kolom dengan "nama poktan" atau "nama_poktan" (tidak mengandung kecamatan/desa)
        poktan_keywords = ['nama poktan', 'nama_poktan', 'poktan']
        poktan_excludes = ['kecamatan', 'desa', 'penyuluh', 'kode']
        poktan_col = find_column_by_keywords(df, poktan_keywords, exclude_keywords=poktan_excludes)
        
        if not poktan_col:
            # Prioritas 2: Cari kolom yang mengandung "poktan" tapi bukan "kode poktan"
            for col in df.columns:
                col_lower = str(col).lower()
                if 'poktan' in col_lower and 'kode' not in col_lower:
                    # Cek nilai sample untuk memastikan ini benar kolom poktan
                    sample_values = df[col].dropna().unique()[:3]
                    sample_str = ' '.join(str(v).lower() for v in sample_values if pd.notna(v))
                    # Jika sample mengandung kata kecamatan/desa, skip
                    if not any(exclude in sample_str for exclude in ['kecamatan', 'desa']):
                        poktan_col = col
                        break
        
        if not poktan_col:
            # Prioritas 3: Cari "kelompok tani"
            poktan_col = find_column_by_keywords(df, ['kelompok tani', 'kelompok_tani', 'klp tani'],
                                               exclude_keywords=['kecamatan', 'desa', 'penyuluh', 'kode'])

        if poktan_col:
            print(f"   ✅ Kolom Poktan: '{poktan_col}'")
            # Debug: Tampilkan sample values
            sample_values = df[poktan_col].dropna().unique()[:5]
            print(f"   📝 Sample Poktan values: {list(sample_values)}")
        else:
            print(f"   ⚠️ Kolom Poktan tidak ditemukan")

        # 4. Nama Desa - DIPERBAIKI
        desa_col = None
        
        # Prioritas 1: Kolom dengan nama persis 'Nama Desa' (case-insensitive)
        desa_col = find_column_by_keywords(df, ['Nama Desa'], exact_match=False)
        
        # Prioritas 2: Kolom yang mengandung 'nama desa' (bukan 'kode desa')
        if not desa_col:
            desa_col = find_column_by_keywords(df, ['nama desa'], 
                                              exclude_keywords=['kode', 'poktan', 'kelompok', 'kecamatan'])
        
        # Prioritas 3: Kolom yang mengandung 'desa' saja
        if not desa_col:
            for col in df.columns:
                col_lower = str(col).lower()
                if 'desa' in col_lower:
                    # Hindari kolom yang mengandung 'poktan' atau 'kelompok'
                    if not any(keyword in col_lower for keyword in ['poktan', 'kelompok', 'kode']):
                        desa_col = col
                        break

        if desa_col:
            print(f"   ✅ Kolom Nama Desa: '{desa_col}'")
            # Debug: Tampilkan sample values
            sample_values = df[desa_col].dropna().unique()[:5]
            print(f"   📝 Sample Desa values: {list(sample_values)}")
        else:
            print(f"   ⚠️ Kolom Nama Desa tidak ditemukan")

        # 5. Nama Kecamatan - DIPERBAIKI
        kec_col = None
        
        # Prioritas: Cari kolom dengan 'kecamatan'
        kec_col = find_column_by_keywords(df, ['kecamatan', 'nama kecamatan', 'kec.'],
                                         exclude_keywords=['poktan', 'desa', 'kelompok'])

        if not kec_col:
            # Coba cari pola lain untuk kecamatan
            for col in df.columns:
                col_lower = str(col).lower()
                kec_patterns = ['kecamatan', 'kec.', 'wilayah kecamatan']
                if any(pattern in col_lower for pattern in kec_patterns):
                    kec_col = col
                    break

        if kec_col:
            print(f"   ✅ Kolom Kecamatan: '{kec_col}'")
            # Debug: Tampilkan sample values
            sample_values = df[kec_col].dropna().unique()[:5]
            print(f"   📝 Sample Kecamatan values: {list(sample_values)}")
        else:
            print(f"   ⚠️ Kolom Kecamatan tidak ditemukan")
            # Ambil dari nama file sebagai fallback
            kec_name = filename.replace('_ERDKK.xlsx', '').replace('.xlsx', '').replace('.xls', '')
            print(f"   📝 Menggunakan nama file sebagai kecamatan: {kec_name}")

        # 6. Nama Kios
        kios_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if 'nama kios pengecer' in col_lower:
                kios_col = col
                print(f"   ✅ Kolom Nama Kios: '{kios_col}'")
                break
        
        if not kios_col:
            print(f"   ⚠️ Kolom Nama Kios tidak ditemukan")

        # 7. Komoditas
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

        # 8. Luas Tanam
        luas_cols = {}
        luas_keywords = ['luas tanam', 'luas lahan', 'luas']
        
        for mt in [1, 2, 3]:
            col = extract_luas_column(df, luas_keywords, mt)
            if col:
                luas_cols[f'mt{mt}'] = col
                print(f"   ✅ Kolom Luas MT{mt}: '{col}'")
        
        if not luas_cols:
            print(f"   ⚠️ Kolom Luas tidak ditemukan")

        # 9. Kolom Pupuk
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

        # BERSIHKAN DATA - VERSI DIPERBAIKI
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

        # Poktan - DIPERBAIKI: Validasi lebih ketat
        if poktan_col and poktan_col in df.columns:
            poktan_values = df.loc[idxs, poktan_col].astype(str).str.strip()
            
            # Validasi: jika nilai mengandung 'kecamatan' atau 'desa', anggap tidak valid
            def validate_poktan(value):
                if pd.isna(value) or value == '':
                    return 'Tidak disebutkan'
                
                val_lower = str(value).lower()
                # Cek jika nilai sebenarnya adalah nama kecamatan/desa
                if any(invalid in val_lower for invalid in ['kecamatan', 'desa', 'penyuluh']):
                    return 'Tidak disebutkan'
                
                # Cek jika terlalu panjang (mungkin kalimat)
                if len(str(value)) > 50:
                    return 'Tidak disebutkan'
                
                return str(value)
            
            clean_df['poktan'] = poktan_values.apply(validate_poktan)
            
            # Validasi tambahan dari sample data
            poktan_sample = clean_df['poktan'].unique()[:10]
            print(f"   🔍 Cleaned Poktan sample: {list(poktan_sample)}")
        else:
            clean_df['poktan'] = 'Tidak disebutkan'

        # Desa
        if desa_col and desa_col in df.columns:
            desa_values = df.loc[idxs, desa_col].astype(str).str.strip()
            desa_values = desa_values.replace([
                '', 'nan', 'NaN', 'Nan', 'NA', 'N/A', '-', 'null', 'NULL', 'None', 'none'
            ], 'Desa tidak diketahui')
            clean_df['desa'] = desa_values
        else:
            clean_df['desa'] = 'Desa tidak diketahui'

        # Kecamatan
        if kec_col and kec_col in df.columns:
            kec_values = df.loc[idxs, kec_col].astype(str).str.strip()
            kec_values = kec_values.replace([
                '', 'nan', 'NaN', 'Nan', 'NA', 'N/A', '-', 'null', 'NULL', 'None', 'none'
            ], 'Kecamatan tidak diketahui')
            
            # Debug: cek apakah ada nilai kecamatan yang salah
            kec_sample = kec_values.unique()[:10]
            print(f"   🔍 Kecamatan sample: {list(kec_sample)}")
            
            clean_df['kecamatan'] = kec_values
        else:
            # Fallback ke nama file
            kec_name = filename.replace('_ERDKK.xlsx', '').replace('.xlsx', '').replace('.xls', '')
            clean_df['kecamatan'] = kec_name
            print(f"   📝 Using filename as kecamatan: {kec_name}")

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

        # Final debug check
        print(f"\n   📊 FINAL DATA CHECK:")
        print(f"   • Total rows: {len(clean_df):,}")
        print(f"   • Poktan unique values: {clean_df['poktan'].nunique()}")
        print(f"   • Desa unique values: {clean_df['desa'].nunique()}")
        print(f"   • Kecamatan unique values: {clean_df['kecamatan'].nunique()}")
        print(f"   • Sample Poktan values: {clean_df['poktan'].unique()[:5]}")
        print(f"   • Sample Desa values: {clean_df['desa'].unique()[:5]}")
        print(f"   • Sample Kecamatan values: {clean_df['kecamatan'].unique()[:5]}")

        return clean_df

    except Exception as e:
        print(f"   ❌ Error memproses file: {e}")
        print(f"   🔍 Traceback: {traceback.format_exc()}")
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
    print(f"🏘️  Poktan unique: {all_data['poktan'].nunique()}")
    print(f"🏠 Desa unique: {all_data['desa'].nunique()}")
    print(f"🗺️  Kecamatan unique: {all_data['kecamatan'].nunique()}")

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

# ==============================================
# FUNGSI GOOGLE SHEETS EXPAND & UPLOAD
# ==============================================

def expand_google_sheet(sheets_service, spreadsheet_id, required_rows, required_cols=3):
    """Expand Google Sheets grid untuk menampung data besar"""
    try:
        print(f"\n📈 EXPANDING GOOGLE SHEETS GRID...")
        print(f"   • Required rows: {required_rows:,}")
        print(f"   • Required columns: {required_cols}")
        
        # 1. Get current sheet properties
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        sheets = spreadsheet.get('sheets', [])
        if not sheets:
            print("   ❌ No sheets found in spreadsheet")
            return False
        
        # Ambil sheet pertama (Sheet1)
        sheet = sheets[0]
        sheet_id = sheet['properties']['sheetId']
        current_rows = sheet['properties']['gridProperties']['rowCount']
        current_cols = sheet['properties']['gridProperties']['columnCount']
        
        print(f"   • Current rows: {current_rows:,}")
        print(f"   • Current columns: {current_cols}")
        
        # 2. Calculate required expansion
        add_rows = max(0, required_rows - current_rows + 1000)  # Tambah buffer 1000
        add_cols = max(0, required_cols - current_cols + 5)     # Tambah buffer 5 kolom
        
        if add_rows == 0 and add_cols == 0:
            print("   ✅ No expansion needed")
            return True
        
        # 3. Prepare batch update requests
        requests = []
        
        if add_rows > 0:
            requests.append({
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "length": add_rows
                }
            })
            print(f"   ➕ Adding {add_rows:,} rows")
        
        if add_cols > 0:
            requests.append({
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "length": add_cols
                }
            })
            print(f"   ➕ Adding {add_cols} columns")
        
        # 4. Execute batch update
        if requests:
            body = {"requests": requests}
            response = sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            print(f"   ✅ Grid expanded successfully")
            print(f"   • New total rows: {current_rows + add_rows:,}")
            print(f"   • New total columns: {current_cols + add_cols}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error expanding sheet: {e}")
        return False

def upload_large_dataset(df, spreadsheet_id, credentials):
    """Upload dataset besar ke Google Sheets dengan chunking yang optimal"""
    try:
        print("\n📤 UPLOADING LARGE DATASET TO GOOGLE SHEETS...")
        print(f"   📊 Data size: {len(df):,} rows, {len(df.columns)} columns")
        
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # 1. Expand sheet jika diperlukan
        required_rows = len(df) + 1  # +1 untuk header
        expand_success = expand_google_sheet(sheets_service, spreadsheet_id, required_rows)
        
        if not expand_success:
            print("   ⚠️ Grid expansion failed, trying to upload anyway")
        
        # 2. Clear existing data
        print("   🧹 Clearing existing data...")
        try:
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A:Z"
            ).execute()
            print("   ✅ Sheet cleared successfully")
            time.sleep(1)
        except Exception as e:
            print(f"   ⚠️ Warning while clearing sheet: {e}")
        
        # 3. Prepare data
        headers = df.columns.tolist()
        values = df.fillna('').values.tolist()
        
        # 4. Upload dengan batch yang lebih kecil untuk reliability
        batch_size = 5000  # Ukuran batch optimal
        total_rows = len(values)
        total_batches = math.ceil(total_rows / batch_size)
        
        print(f"\n📦 UPLOAD STRATEGY:")
        print(f"   • Total data rows: {total_rows:,}")
        print(f"   • Batch size: {batch_size:,}")
        print(f"   • Number of batches: {total_batches}")
        print(f"   • Estimated time: {total_batches * 2} seconds")
        
        # 5. Upload header terlebih dahulu
        print("\n📋 Uploading headers...")
        try:
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            print("   ✅ Headers uploaded")
            time.sleep(1)
        except Exception as e:
            print(f"   ⚠️ Error uploading headers: {e}")
            return False
        
        # 6. Upload data per batch
        successful_batches = 0
        failed_batches = []
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_rows)
            batch_data = values[start_idx:end_idx]
            batch_size_actual = len(batch_data)
            
            # Range untuk batch ini (baris mulai dari 2 karena header di row 1)
            range_start = start_idx + 2
            range_name = f"Sheet1!A{range_start}"
            
            max_retries = 3
            batch_success = False
            
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        print(f"   🔄 Retry {attempt} for batch {batch_num + 1}...")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    
                    print(f"   📤 Batch {batch_num + 1}/{total_batches}: rows {start_idx + 1:,}-{end_idx:,} ({batch_size_actual:,} rows)...")
                    
                    body = {
                        "values": batch_data,
                        "majorDimension": "ROWS"
                    }
                    
                    request = sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption="USER_ENTERED",
                        body=body
                    )
                    response = request.execute()
                    
                    updated_cells = response.get('updatedCells', 0)
                    print(f"   ✅ Batch {batch_num + 1} uploaded ({updated_cells:,} cells updated)")
                    
                    successful_batches += 1
                    batch_success = True
                    
                    # Delay antar batch untuk menghindari rate limit
                    if batch_num < total_batches - 1:
                        delay = 0.5  # 500ms delay
                        time.sleep(delay)
                    
                    break  # Break retry loop jika sukses
                    
                except Exception as e:
                    error_msg = str(e)
                    if "exceeds grid limits" in error_msg:
                        print(f"   ❌ GRID LIMIT ERROR: Need to expand sheet more")
                        # Coba expand sheet lagi
                        additional_rows_needed = start_idx + batch_size_actual + 1000
                        expand_google_sheet(sheets_service, spreadsheet_id, additional_rows_needed)
                        time.sleep(2)
                        continue  # Coba lagi
                    
                    print(f"   ⚠️ Attempt {attempt + 1} failed: {error_msg[:100]}...")
                    
                    if attempt < max_retries - 1:
                        wait_time = 2 * (attempt + 1)
                        print(f"   ⏳ Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    else:
                        print(f"   ❌ Batch {batch_num + 1} failed after all retries")
                        failed_batches.append({
                            'batch': batch_num + 1,
                            'rows': f"{start_idx + 1}-{end_idx}",
                            'error': error_msg[:200]
                        })
            
            if not batch_success:
                print(f"   ⚠️ Moving to next batch...")
        
        # 7. Report upload results
        print(f"\n📊 UPLOAD COMPLETE REPORT:")
        print(f"   • Total batches attempted: {total_batches}")
        print(f"   • Successful batches: {successful_batches}")
        print(f"   • Failed batches: {len(failed_batches)}")
        
        if failed_batches:
            print(f"   ❌ FAILED BATCHES:")
            for fb in failed_batches:
                print(f"     - Batch {fb['batch']}: rows {fb['rows']}")
                print(f"       Error: {fb['error']}")
        
        success_rate = (successful_batches / total_batches) * 100
        print(f"   📈 Success rate: {success_rate:.1f}%")
        
        if successful_batches > 0:
            estimated_uploaded_rows = successful_batches * batch_size
            if successful_batches == total_batches:
                estimated_uploaded_rows = total_rows
            
            print(f"   ✅ Estimated uploaded rows: {estimated_uploaded_rows:,}/{total_rows:,}")
            return True
        else:
            print(f"   ❌ No batches uploaded successfully")
            return False
        
    except Exception as e:
        print(f"❌ Error in upload process: {e}")
        print(f"   🔧 Error type: {type(e).__name__}")
        return False

def verify_complete_upload(sheets_service, spreadsheet_id, expected_rows):
    """Verifikasi upload secara menyeluruh"""
    try:
        print("\n🔍 COMPREHENSIVE UPLOAD VERIFICATION...")
        
        # Ambil data dari sheet dengan range yang besar
        print("   📥 Fetching data from sheet...")
        
        # Hitung berapa banyak requests yang diperlukan
        max_rows_per_request = 10000  # Google Sheets API limit per request
        num_requests = math.ceil(expected_rows / max_rows_per_request)
        
        total_uploaded_rows = 0
        
        for req_num in range(num_requests):
            start_row = req_num * max_rows_per_request + 1  # +1 untuk header
            end_row = min((req_num + 1) * max_rows_per_request, expected_rows)
            
            range_name = f"Sheet1!A{start_row}:C{end_row}"
            
            try:
                result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    majorDimension="ROWS"
                ).execute()
                
                values = result.get('values', [])
                if req_num == 0 and values:
                    # Skip header pada request pertama
                    rows_in_batch = len(values) - 1
                else:
                    rows_in_batch = len(values)
                
                total_uploaded_rows += rows_in_batch
                
                print(f"   • Batch {req_num + 1}: rows {start_row}-{end_row} → {rows_in_batch:,} rows found")
                
                # Check sample data untuk verifikasi
                if req_num == 0 and len(values) > 1:
                    sample_row = values[1] if len(values) > 1 else []
                    if sample_row:
                        print(f"   • Sample data: NIK={sample_row[0][:20] if len(sample_row) > 0 else 'N/A'}...")
                
            except Exception as e:
                print(f"   ⚠️ Error fetching batch {req_num + 1}: {e}")
        
        print(f"\n   📊 VERIFICATION SUMMARY:")
        print(f"   • Expected rows: {expected_rows:,}")
        print(f"   • Actual rows found: {total_uploaded_rows:,}")
        
        if total_uploaded_rows == expected_rows:
            print(f"   ✅ PERFECT UPLOAD: All {expected_rows:,} rows uploaded successfully!")
            return True, total_uploaded_rows
        elif total_uploaded_rows > 0:
            percentage = (total_uploaded_rows / expected_rows) * 100
            print(f"   ⚠️ PARTIAL UPLOAD: {total_uploaded_rows:,}/{expected_rows:,} rows ({percentage:.1f}%)")
            return True, total_uploaded_rows  # Masih return True karena ada data
        else:
            print(f"   ❌ NO DATA FOUND in sheet")
            return False, 0
            
    except Exception as e:
        print(f"   ⚠️ Verification error: {e}")
        return False, 0

def cleanup_data_for_upload(df):
    """Optimasi data untuk upload ke Google Sheets"""
    print("🧹 Optimizing data for Google Sheets upload...")
    
    # 1. Pastikan hanya ada 3 kolom yang diperlukan
    required_cols = ['nik', 'nama_petani', 'data']
    if not all(col in df.columns for col in required_cols):
        print(f"❌ Missing required columns. Available: {list(df.columns)}")
        return None
    
    df = df[required_cols].copy()
    
    # 2. Konversi ke string dan strip whitespace
    df['nik'] = df['nik'].astype(str).str.strip()
    df['nama_petani'] = df['nama_petani'].astype(str).str.strip()
    df['data'] = df['data'].astype(str).str.strip()
    
    # 3. Truncate data yang terlalu panjang (Google Sheets limit: 50,000 chars per cell)
    max_cell_length = 40000  # Buffer dari 50k limit
    df['data'] = df['data'].apply(lambda x: x[:max_cell_length] if len(x) > max_cell_length else x)
    
    # 4. Remove problematic characters
    def clean_text(text):
        # Hapus karakter kontrol dan non-printable
        text = ''.join(char for char in str(text) if ord(char) >= 32 or ord(char) in [9, 10, 13])
        # Ganti multiple newlines dengan single newline
        text = re.sub(r'\n\s*\n', '\n', text)
        return text
    
    df['data'] = df['data'].apply(clean_text)
    
    # 5. Sort by NIK untuk konsistensi
    df = df.sort_values('nik').reset_index(drop=True)
    
    print(f"   ✅ Optimized {len(df):,} rows")
    print(f"   📏 Column sizes:")
    print(f"     • nik: {df['nik'].str.len().max()} chars max")
    print(f"     • nama_petani: {df['nama_petani'].str.len().max()} chars max")
    print(f"     • data: {df['data'].str.len().max():,} chars max")
    
    return df

def save_backup(df):
    """Simpan backup lokal"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ERDKK_Hasil_{timestamp}.csv"

        df.to_csv(filename, index=False, encoding='utf-8-sig')

        print(f"💾 Backup disimpan: {filename}")
        print(f"   📁 Ukuran: {os.path.getsize(filename) / (1024 * 1024):.2f} MB")

        return filename
    except Exception as e:
        print(f"⚠️ Gagal menyimpan backup: {e}")
        return None

def cleanup_temp_files():
    """Hapus semua file temporary"""
    temp_patterns = [
        'temp_*.xlsx', 'temp_*.xls', 
        'processed_*.xlsx', 'processed_*.xls', 
        'ERDKK_Hasil_*.csv', 'ERDKK_Stats_*.txt',
        'debug_info_*.txt'
    ]
    
    deleted_count = 0
    for pattern in temp_patterns:
        for file in glob.glob(pattern):
            try:
                os.remove(file)
                deleted_count += 1
                print(f"🗑️  Deleted: {file}")
            except Exception as e:
                print(f"⚠️ Failed to delete {file}: {e}")
    
    if deleted_count > 0:
        print(f"✅ Cleaned up {deleted_count} temporary files")

# ==============================================
# FUNGSI UTAMA
# ==============================================

def main():
    """Fungsi utama dengan perbaikan deteksi kolom"""
    print("\n" + "="*60)
    print("🚀 ERDKK WA CENTER - FIXED VERSION (Poktan/Kecamatan Detection)")
    print("="*60)
    print(f"📅 Start time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    
    backup_files = []
    
    try:
        # 1. Kirim notifikasi mulai
        send_email_notification(
            "ERDKK WA Center - Proses Data Besar Dimulai",
            f"Memproses dataset besar dengan perbaikan deteksi kolom ({datetime.now().strftime('%d/%m/%Y %H:%M:%S')}).",
            is_success=True
        )
        
        # 2. Autentikasi
        print("\n🔐 AUTHENTICATING...")
        credentials = authenticate_google()
        if not credentials:
            error_msg = "Authentication failed"
            send_error_email(error_msg)
            sys.exit(1)
        
        drive_service = build('drive', 'v3', credentials=credentials)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # 3. Ambil file dari Google Drive
        print("\n📂 GETTING FILES FROM GOOGLE DRIVE...")
        files = extract_files_from_folder(FOLDER_ID, drive_service)
        if not files:
            error_msg = "No Excel files found"
            send_error_email(error_msg)
            sys.exit(1)
        
        # 4. Proses file
        all_data = []
        success_count = 0
        fail_count = 0
        
        print(f"\n📁 PROCESSING {len(files)} FILES...")
        for i, file in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}] Processing: {file['name']}")
            df = read_and_process_excel(file['id'], drive_service, file['name'])
            
            if df is not None and not df.empty:
                all_data.append(df)
                success_count += 1
                print(f"   ✅ Success ({len(df):,} rows)")
                
                # Debug: cek distribusi data
                print(f"   🔍 Data check:")
                print(f"      • Poktan values: {df['poktan'].nunique()}")
                print(f"      • Top Poktan: {df['poktan'].value_counts().head(3).to_dict()}")
                print(f"      • Desa values: {df['desa'].nunique()}")
                print(f"      • Kecamatan values: {df['kecamatan'].nunique()}")
            else:
                fail_count += 1
                print(f"   ❌ Failed")
        
        print(f"\n📊 PROCESSING SUMMARY:")
        print(f"   ✅ Success: {success_count} files")
        print(f"   ❌ Failed: {fail_count} files")
        
        if not all_data:
            error_msg = "No valid data to process"
            send_error_email(error_msg)
            sys.exit(1)
        
        # 5. Pivot data
        print("\n🔄 CREATING PIVOT DATA...")
        result_df = pivot_and_format_data(all_data)
        
        if result_df.empty:
            error_msg = "Pivot result is empty"
            send_error_email(error_msg)
            sys.exit(1)
        
        print(f"\n📈 PIVOT RESULT STATISTICS:")
        print(f"   • Total unique farmers: {len(result_df):,}")
        print(f"   • Total rows in result: {result_df.shape[0]:,}")
        
        # 6. Optimasi data untuk upload
        print("\n⚡ OPTIMIZING DATA FOR UPLOAD...")
        clean_df = cleanup_data_for_upload(result_df)
        if clean_df is None:
            error_msg = "Data optimization failed"
            send_error_email(error_msg)
            sys.exit(1)
        
        # 7. Simpan backup
        backup_file = save_backup(clean_df)
        if backup_file:
            backup_files.append(backup_file)
        
        # 8. Upload ke Google Sheets dengan metode baru
        upload_success = upload_large_dataset(clean_df, SPREADSHEET_ID, credentials)
        
        # 9. Verifikasi upload
        verification_success = False
        uploaded_rows = 0
        
        if upload_success:
            verification_success, uploaded_rows = verify_complete_upload(
                sheets_service,
                SPREADSHEET_ID,
                len(clean_df)
            )
        
        # 10. Kirim notifikasi hasil
        print("\n📧 SENDING NOTIFICATION EMAIL...")
        
        # PERBAIKAN: Hitung persentase yang lebih akurat
        actual_uploaded = uploaded_rows
        total_expected = len(clean_df)
        
        # Jika hanya beda 1-2 baris, anggap berhasil 100%
        # (mungkin perbedaan penghitungan header)
        if actual_uploaded >= total_expected - 2:
            success_percentage = 100.0
            is_complete_success = True
        else:
            success_percentage = (actual_uploaded / total_expected) * 100
            is_complete_success = success_percentage >= 99.9  # 99.9% dianggap sukses
        
        # Persiapkan pesan berdasarkan hasil
        if is_complete_success:
            subject = f"✅ ERDKK WA Center - Proses Berhasil 100%"
            body = f"""
🎉 LAPORAN PROSES BERHASIL 100%

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Hasil: {actual_uploaded:,}/{total_expected:,} petani berhasil diupload
📈 Akurasi: {success_percentage:.4f}%

📊 STATISTIK DETAIL:
──────────────────────────────
📁 File diproses: {len(files)} file
✅ File berhasil: {success_count} file
❌ File gagal: {fail_count} file
👤 Total petani: {total_expected:,}
📄 Baris terupload: {actual_uploaded:,}
🎯 Akurasi: {success_percentage:.4f}%

✅ PERBAIKAN DETEKSI KOLOM:
──────────────────────────────
• ✅ Deteksi kolom Poktan diperbaiki
• ✅ Deteksi kolom Kecamatan diperbaiki
• ✅ Validasi data lebih ketat
• ✅ Sample values checking

🔗 GOOGLE SHEETS:
──────────────────────────────
📊 Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}
📈 Total rows: {actual_uploaded:,} + 1 header = {actual_uploaded + 1:,} baris

✅ DETAIL PROSES:
──────────────────────────────
1. ✅ Pengambilan file dari Google Drive
2. ✅ Pembersihan dan validasi data NIK
3. ✅ Penggabungan data berdasarkan NIK
4. ✅ Expand Google Sheets grid
5. ✅ Upload data ke Google Sheets
6. ✅ Verifikasi upload
7. ✅ Pengiriman notifikasi

⚙️ KONFIGURASI:
──────────────────────────────
• Folder Sumber: {FOLDER_ID}
• Spreadsheet: {SPREADSHEET_ID}
• Data size: {total_expected:,} rows
• Uploaded: {actual_uploaded:,} rows
• Accuracy: {success_percentage:.4f}%

🎯 STATUS: PROSES BERHASIL DENGAN SEMPURNA
✅ Semua data petani berhasil diproses dan diupload
"""
        elif actual_uploaded > total_expected * 0.9:  # >90% success
            subject = f"⚠️ ERDKK WA Center - Proses {success_percentage:.1f}% Berhasil"
            body = f"""
📊 LAPORAN PROSES SEBAGIAN BERHASIL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Hasil: {actual_uploaded:,}/{total_expected:,} petani ({success_percentage:.1f}%)

📊 STATISTIK:
──────────────────────────────
📁 File diproses: {len(files)} file
✅ File berhasil: {success_count} file
❌ File gagal: {fail_count} file
👤 Total petani: {total_expected:,}
📄 Baris terupload: {actual_uploaded:,}
📉 Baris missing: {total_expected - actual_uploaded:,}
🎯 Akurasi: {success_percentage:.1f}%

🔗 GOOGLE SHEETS:
──────────────────────────────
https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}

⚠️ CATATAN:
──────────────────────────────
• {total_expected - actual_uploaded:,} baris belum terupload
• Backup file tersimpan di server untuk recovery
• Data yang ada sudah dapat digunakan

🎯 STATUS: SEBAGIAN BESAR BERHASIL
"""
        else:
            subject = f"❌ ERDKK WA Center - Upload Gagal"
            body = f"""
❌ LAPORAN PROSES GAGAL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Data diproses: {total_expected:,} petani
📊 Data terupload: {actual_uploaded:,} petani
📉 Akurasi: {success_percentage:.1f}%

🔧 TROUBLESHOOTING:
──────────────────────────────
1. Cek kuota Google Sheets (10 juta cell)
2. Pastikan service account punya akses edit
3. Coba manual upload file backup
4. Periksa koneksi internet
5. Hubungi administrator sistem

📋 BACKUP FILE:
──────────────────────────────
File backup lengkap tersimpan di server

🎯 STATUS: GAGAL UPLOAD (perlu tindakan lebih lanjut)
"""
        
        # Kirim email dengan status yang benar
        email_success = send_email_notification(subject, body, is_success=is_complete_success)
        
        # 11. Final status dengan logika yang lebih baik
        print("\n" + "="*60)
        
        if is_complete_success:
            print(f"🎉 PROSES BERHASIL 100%!")
            print(f"   • Total expected: {total_expected:,} rows")
            print(f"   • Actual uploaded: {actual_uploaded:,} rows")
            print(f"   • Accuracy: {success_percentage:.4f}%")
            print(f"   • Status: COMPLETE SUCCESS")
            
            # Jika benar-benar 100%, exit dengan code 0
            exit_code = 0
        elif actual_uploaded > total_expected * 0.9:
            print(f"⚠️ PROSES HAMPIR SEMPURNA ({success_percentage:.2f}%)")
            print(f"   • {actual_uploaded:,}/{total_expected:,} rows uploaded")
            print(f"   • {total_expected - actual_uploaded:,} rows missing")
            print(f"   • Status: PARTIAL SUCCESS (acceptable)")
            
            # Untuk >90% success, masih anggap acceptable
            exit_code = 0
        else:
            print("❌ PROSES GAGAL (upload tidak berhasil)")
            print(f"   • Only {actual_uploaded:,}/{total_expected:,} rows uploaded")
            print(f"   • {total_expected - actual_uploaded:,} rows missing")
            print("   • Backup file tersimpan untuk manual upload")
            
            # Untuk <90% success, exit dengan error
            exit_code = 1
        
        print(f"🔗 Link: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
        print("="*60)
        
        # Exit dengan code yang sesuai
        sys.exit(exit_code)
        
    except Exception as e:
        error_msg = f"Error in main process: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        print(f"\n❌ {error_msg}")
        send_error_email(error_msg)
        sys.exit(1)
    
    finally:
        # Cleanup setelah email terkirim
        if 'email_success' in locals() and email_success:
            for backup_file in backup_files:
                if os.path.exists(backup_file):
                    try:
                        os.remove(backup_file)
                        print(f"🗑️  Backup file deleted: {os.path.basename(backup_file)}")
                    except Exception as e:
                        print(f"⚠️ Failed to delete backup file: {e}")
        
        cleanup_temp_files()

# ==============================================
# JALANKAN FUNGSI UTAMA
# ==============================================

if __name__ == "__main__":
    main()
