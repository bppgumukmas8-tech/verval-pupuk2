#!/usr/bin/env python3
"""
erdkk_wa_center.py - VERSI DIPERBAIKI
Memperbaiki masalah data tidak lengkap saat upload
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
import time  # Tambahkan ini

# ==============================================
# KONFIGURASI
# ==============================================
FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"
SPREADSHEET_ID = "1nrZ1YLMijIrmHA3hJUw5AsdElkTH1oIxt3ux2mbdTn8"

# ==============================================
# LOAD EMAIL CONFIGURATION FROM SECRETS
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

# ==============================================
# FUNGSI UTAMA YANG DIPERBAIKI
# ==============================================

def upload_to_google_sheets_improved(df, spreadsheet_id, credentials):
    """Upload ke Google Sheets dengan retry logic dan chunking lebih baik"""
    try:
        print("\n📤 MENGUPLOAD KE GOOGLE SHEETS (VERSION IMPROVED)...")
        print(f"   📊 Data size: {len(df)} rows, {len(df.columns)} columns")
        
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # 1. Clear existing data
        print("   🧹 Clearing existing sheet...")
        try:
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A:Z"
            ).execute()
            print("   ✅ Sheet cleared successfully")
            time.sleep(1)  # Tunggu sebentar
        except Exception as e:
            print(f"   ⚠️ Warning saat clear sheet: {e}")
        
        # 2. Prepare data
        headers = df.columns.tolist()
        values = df.fillna('').values.tolist()
        data = [headers] + values
        
        # 3. Upload dengan retry logic
        max_retries = 3
        batch_size = 1000  # Lebih kecil untuk menghindari timeout
        
        if len(data) <= batch_size:
            # Upload sekaligus untuk data kecil
            for attempt in range(max_retries):
                try:
                    print(f"   📤 Upload attempt {attempt + 1}...")
                    body = {
                        "values": data,
                        "majorDimension": "ROWS"
                    }
                    
                    request = sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range="Sheet1!A1",
                        valueInputOption="USER_ENTERED",
                        body=body
                    )
                    response = request.execute()
                    
                    print(f"   ✅ Berhasil mengupload {len(data)} rows")
                    print(f"   📋 Cells updated: {response.get('updatedCells', 'N/A')}")
                    return True
                    
                except Exception as e:
                    print(f"   ⚠️ Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        print(f"   ⏳ Retrying in 3 seconds...")
                        time.sleep(3)
                    else:
                        print(f"   ❌ All upload attempts failed")
                        
        else:
            # Upload per batch untuk data besar
            print(f"   📦 Data besar: {len(data)-1} rows, akan di-upload per {batch_size} rows")
            
            # Upload header terlebih dahulu
            print("   📋 Uploading headers...")
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
            
            # Upload data per batch
            total_batches = (len(values) + batch_size - 1) // batch_size
            successful_batches = 0
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(values))
                batch_data = values[start_idx:end_idx]
                
                # Range untuk batch ini
                range_start = start_idx + 2  # +2 karena row 1 header
                range_name = f"Sheet1!A{range_start}"
                
                for attempt in range(max_retries):
                    try:
                        print(f"   📤 Batch {batch_num + 1}/{total_batches}: rows {start_idx + 1}-{end_idx}...")
                        
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
                        
                        print(f"   ✅ Batch {batch_num + 1} uploaded successfully")
                        successful_batches += 1
                        time.sleep(0.5)  # Tunggu sebentar antar batch
                        break
                        
                    except Exception as e:
                        print(f"   ⚠️ Batch {batch_num + 1} attempt {attempt + 1} failed: {e}")
                        if attempt < max_retries - 1:
                            print(f"   ⏳ Retrying in 2 seconds...")
                            time.sleep(2)
                        else:
                            print(f"   ❌ Batch {batch_num + 1} failed after all retries")
                
            print(f"\n   📊 Batch upload summary:")
            print(f"   • Total batches: {total_batches}")
            print(f"   • Successful: {successful_batches}")
            print(f"   • Failed: {total_batches - successful_batches}")
            
            if successful_batches > 0:
                print(f"   ✅ Partial success: {successful_batches}/{total_batches} batches uploaded")
                return True
            else:
                print(f"   ❌ All batches failed")
                return False
        
        return False
        
    except Exception as e:
        print(f"❌ Error dalam upload process: {e}")
        print(f"   🔧 Error type: {type(e).__name__}")
        return False

def verify_upload(sheets_service, spreadsheet_id, expected_rows):
    """Verifikasi data yang sudah terupload"""
    try:
        print("\n🔍 VERIFYING UPLOAD...")
        
        # Ambil data dari sheet
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:C"
        ).execute()
        
        uploaded_values = result.get('values', [])
        uploaded_rows = len(uploaded_values) - 1  # Exclude header
        
        print(f"   📊 Upload verification:")
        print(f"   • Expected rows: {expected_rows}")
        print(f"   • Actual rows in sheet: {uploaded_rows}")
        
        if uploaded_rows == expected_rows:
            print(f"   ✅ VERIFICATION PASSED: All data uploaded successfully")
            return True
        elif uploaded_rows > 0:
            print(f"   ⚠️ PARTIAL SUCCESS: {uploaded_rows}/{expected_rows} rows uploaded")
            return True
        else:
            print(f"   ❌ VERIFICATION FAILED: No data found in sheet")
            return False
            
    except Exception as e:
        print(f"   ⚠️ Verification error: {e}")
        return False

def cleanup_dataframe(df):
    """Bersihkan dataframe sebelum upload"""
    print("🧹 Cleaning dataframe for upload...")
    
    # 1. Pastikan hanya ada 3 kolom yang diperlukan
    required_cols = ['nik', 'nama_petani', 'data']
    if not all(col in df.columns for col in required_cols):
        print(f"❌ Missing required columns. Available: {list(df.columns)}")
        return None
    
    df = df[required_cols].copy()
    
    # 2. Bersihkan data NIK
    df['nik'] = df['nik'].astype(str).str.strip()
    
    # 3. Batasi panjang string untuk nama_petani dan data
    df['nama_petani'] = df['nama_petani'].astype(str).str.strip()
    df['data'] = df['data'].astype(str).str.strip()
    
    # 4. Hilangkan karakter non-ASCII yang bermasalah
    df['data'] = df['data'].apply(lambda x: ''.join([c for c in str(x) if ord(c) < 128 or ord(c) > 159]))
    
    # 5. Batasi panjang maksimum data (Google Sheets cell limit: 50,000 chars)
    df['data'] = df['data'].apply(lambda x: x[:40000] if len(str(x)) > 40000 else x)
    
    print(f"   ✅ Cleaned {len(df)} rows")
    return df

def save_debug_info(df, files_processed):
    """Simpan informasi debug untuk troubleshooting"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_file = f"debug_info_{timestamp}.txt"
        
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(f"ERDKK WA Center - Debug Information\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("="*60 + "\n\n")
            
            f.write(f"📊 DATA STATISTICS:\n")
            f.write(f"• Total rows: {len(df)}\n")
            f.write(f"• Total columns: {len(df.columns)}\n")
            f.write(f"• Columns: {list(df.columns)}\n\n")
            
            f.write(f"📁 FILE PROCESSING:\n")
            f.write(f"• Files processed: {files_processed}\n\n")
            
            f.write(f"📋 SAMPLE DATA (first 5 rows):\n")
            for i, row in df.head().iterrows():
                f.write(f"\nRow {i+1}:\n")
                f.write(f"  NIK: {row.get('nik', 'N/A')}\n")
                f.write(f"  Nama: {row.get('nama_petani', 'N/A')}\n")
                f.write(f"  Data length: {len(str(row.get('data', '')))}\n")
            
            f.write(f"\n📏 COLUMN LENGTHS:\n")
            f.write(f"• nik max length: {df['nik'].astype(str).str.len().max()}\n")
            f.write(f"• nama_petani max length: {df['nama_petani'].astype(str).str.len().max()}\n")
            f.write(f"• data max length: {df['data'].astype(str).str.len().max()}\n")
        
        print(f"💾 Debug info saved: {debug_file}")
        return debug_file
    except Exception as e:
        print(f"⚠️ Failed to save debug info: {e}")
        return None

# ==============================================
# FUNGSI UTAMA YANG DIPERBAIKI
# ==============================================

def main_improved():
    """Fungsi utama yang diperbaiki"""
    print("\n" + "="*60)
    print("🚀 ERDKK WA CENTER - VERSION IMPROVED")
    print("="*60)
    print(f"📅 Start time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    
    try:
        # 1. Kirim notifikasi mulai
        send_email_notification(
            "ERDKK WA Center - Proses Dimulai",
            f"Proses pivot data dimulai pada {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}.",
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
        
        for i, file in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}] Processing: {file['name']}")
            df = read_and_process_excel(file['id'], drive_service, file['name'])
            
            if df is not None and not df.empty:
                all_data.append(df)
                success_count += 1
                print(f"   ✅ Success ({len(df)} rows)")
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
        
        print(f"\n📈 PIVOT RESULT:")
        print(f"   • Total unique farmers: {len(result_df)}")
        print(f"   • Total rows: {result_df.shape[0]}")
        
        # 6. Bersihkan dataframe sebelum upload
        clean_df = cleanup_dataframe(result_df)
        if clean_df is None:
            error_msg = "Data cleaning failed"
            send_error_email(error_msg)
            sys.exit(1)
        
        # 7. Simpan debug info
        debug_file = save_debug_info(clean_df, len(files))
        
        # 8. Upload ke Google Sheets dengan metode baru
        upload_success = upload_to_google_sheets_improved(
            clean_df, 
            SPREADSHEET_ID, 
            credentials
        )
        
        # 9. Verifikasi upload
        if upload_success:
            verification_success = verify_upload(
                sheets_service,
                SPREADSHEET_ID,
                len(clean_df)
            )
        else:
            verification_success = False
        
        # 10. Kirim notifikasi hasil
        print("\n📧 SENDING NOTIFICATION EMAIL...")
        
        # Persiapkan pesan berdasarkan hasil
        if upload_success and verification_success:
            subject = f"✅ ERDKK WA Center - Proses Berhasil - {datetime.now().strftime('%d/%m/%Y')}"
            body = f"""
📊 LAPORAN PROSES BERHASIL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Hasil: {len(clean_df)} petani berhasil diproses
📁 File diproses: {len(files)} file
✅ Berhasil: {success_count} file
❌ Gagal: {fail_count} file

🔗 Google Sheets: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}

Status: SEMUA DATA BERHASIL DIUPLOAD
"""
        elif upload_success:
            subject = f"⚠️ ERDKK WA Center - Proses Sebagian Berhasil - {datetime.now().strftime('%d/%m/%Y')}"
            body = f"""
📊 LAPORAN PROSES SEBAGIAN BERHASIL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Data diproses: {len(clean_df)} petani
⚠️ Status: Data terupload tetapi perlu verifikasi manual

🔗 Google Sheets: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}

📋 File debug tersedia di server untuk troubleshooting.
"""
        else:
            subject = f"❌ ERDKK WA Center - Upload Gagal - {datetime.now().strftime('%d/%m/%Y')}"
            body = f"""
📊 LAPORAN PROSES GAGAL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Data diproses: {len(clean_df)} petani
❌ Status: Upload ke Google Sheets gagal

🔧 Troubleshooting:
• Periksa koneksi internet
• Pastikan Google Sheets tidak full (max 5 juta cell)
• Periksa ukuran data (max 50,000 char per cell)

📋 File debug tersedia di server: {debug_file}
"""
        
        # Kirim email
        email_sent = send_email_notification(subject, body, is_success=upload_success)
        
        # 11. Simpan backup (selalu lakukan)
        backup_file = save_backup(clean_df)
        
        # 12. Cleanup
        if debug_file and os.path.exists(debug_file):
            os.remove(debug_file)
            print(f"🗑️  Debug file cleaned up")
        
        if backup_file and os.path.exists(backup_file):
            os.remove(backup_file)
            print(f"🗑️  Backup file cleaned up")
        
        # 13. Final status
        print("\n" + "="*60)
        if upload_success and verification_success:
            print("🎉 PROSES BERHASIL DENGAN SEMPURNA!")
        elif upload_success:
            print("⚠️ PROSES SEBAGIAN BERHASIL (perlu verifikasi)")
        else:
            print("❌ PROSES GAGAL (upload tidak berhasil)")
            sys.exit(1)
        
        print("="*60)
        
    except Exception as e:
        error_msg = f"Error in main process: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        print(f"\n❌ {error_msg}")
        send_error_email(error_msg)
        sys.exit(1)

# ==============================================
# JALANKAN FUNGSI UTAMA YANG DIPERBAIKI
# ==============================================

if __name__ == "__main__":
    # Jalankan versi yang diperbaiki
    main_improved()
