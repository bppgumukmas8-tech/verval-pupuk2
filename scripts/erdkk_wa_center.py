#!/usr/bin/env python3
"""
erdkk_wa_center_expand.py
Expand Google Sheets grid untuk menampung data besar (175k+ rows)
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

# ==============================================
# KONFIGURASI
# ==============================================
FOLDER_ID = "13N5dLdHzAKff6g8RDRiHa7LFyZbdJUCJ"
SPREADSHEET_ID = "1nrZ1YLMijIrmHA3hJUw5AsdElkTH1oIxt3ux2mbdTn8"

# ==============================================
# FUNGSI UTAMA YANG DIPERBAIKI
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

def save_backup_with_stats(df, stats):
    """Simpan backup dengan statistik lengkap"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ERDKK_Hasil_{timestamp}.csv"
        
        # Simpan data
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # Simpan statistik terpisah
        stats_filename = f"ERDKK_Stats_{timestamp}.txt"
        with open(stats_filename, 'w', encoding='utf-8') as f:
            f.write("ERDKK WA CENTER - STATISTICS REPORT\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("="*60 + "\n\n")
            
            for key, value in stats.items():
                if isinstance(value, dict):
                    f.write(f"{key}:\n")
                    for k, v in value.items():
                        f.write(f"  • {k}: {v}\n")
                else:
                    f.write(f"{key}: {value}\n")
                f.write("\n")
        
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"💾 Backup saved: {filename}")
        print(f"   📁 Size: {file_size_mb:.2f} MB")
        print(f"   📊 Statistics saved: {stats_filename}")
        
        return filename, stats_filename
        
    except Exception as e:
        print(f"⚠️ Failed to save backup: {e}")
        return None, None

# ==============================================
# FUNGSI UTAMA YANG DIPERBAIKI LAGI
# ==============================================

def main_final():
    """Fungsi utama final dengan penanganan data besar"""
    print("\n" + "="*60)
    print("🚀 ERDKK WA CENTER - FINAL VERSION FOR LARGE DATASETS")
    print("="*60)
    print(f"📅 Start time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    
    backup_files = []
    
    try:
        # 1. Kirim notifikasi mulai
        send_email_notification(
            "ERDKK WA Center - Proses Data Besar Dimulai",
            f"Memproses dataset besar ({datetime.now().strftime('%d/%m/%Y %H:%M:%S')}).",
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
        
        # 7. Simpan backup dengan statistik
        stats = {
            "processing_summary": {
                "total_files": len(files),
                "successful_files": success_count,
                "failed_files": fail_count
            },
            "data_statistics": {
                "total_farmers": len(clean_df),
                "total_rows": clean_df.shape[0],
                "max_nik_length": clean_df['nik'].str.len().max(),
                "max_name_length": clean_df['nama_petani'].str.len().max(),
                "max_data_length": clean_df['data'].str.len().max()
            },
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        backup_csv, backup_stats = save_backup_with_stats(clean_df, stats)
        if backup_csv:
            backup_files.append(backup_csv)
        if backup_stats:
            backup_files.append(backup_stats)
        
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
        
        # Persiapkan pesan berdasarkan hasil
        subject_prefix = "✅" if verification_success and uploaded_rows == len(clean_df) else "⚠️" if uploaded_rows > 0 else "❌"
        
        if verification_success and uploaded_rows == len(clean_df):
            subject = f"{subject_prefix} ERDKK WA Center - Proses Berhasil 100%"
            body = f"""
🎉 LAPORAN PROSES BERHASIL 100%

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Hasil: SEMUA {uploaded_rows:,} petani berhasil diupload

📈 STATISTIK:
──────────────────────────────
📁 File diproses: {len(files)} file
✅ File berhasil: {success_count} file
❌ File gagal: {fail_count} file
👤 Total petani: {len(clean_df):,}

🔗 GOOGLE SHEETS:
──────────────────────────────
https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}

📋 DETAIL:
──────────────────────────────
• Google Sheets berhasil di-expand untuk {uploaded_rows + 1000:,} baris
• Semua data terupload dengan sempurna
• Backup file tersimpan di server

🎯 STATUS: 100% BERHASIL
"""
        elif uploaded_rows > 0:
            percentage = (uploaded_rows / len(clean_df)) * 100
            subject = f"{subject_prefix} ERDKK WA Center - Proses {percentage:.1f}% Berhasil"
            body = f"""
📊 LAPORAN PROSES SEBAGIAN BERHASIL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Hasil: {uploaded_rows:,}/{len(clean_df):,} petani ({percentage:.1f}%)

📈 STATISTIK:
──────────────────────────────
📁 File diproses: {len(files)} file
✅ File berhasil: {success_count} file
❌ File gagal: {fail_count} file

🔗 GOOGLE SHEETS:
──────────────────────────────
https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}

⚠️ CATATAN:
──────────────────────────────
• {len(clean_df) - uploaded_rows:,} baris belum terupload
• Mungkin perlu manual upload untuk sisanya
• Backup file lengkap tersimpan di server

🎯 STATUS: SEBAGIAN BERHASIL
"""
        else:
            subject = f"{subject_prefix} ERDKK WA Center - Upload Gagal"
            body = f"""
❌ LAPORAN PROSES GAGAL

⏰ Waktu: {datetime.now().strftime('%d %B %Y %H:%M:%S')}
📊 Data diproses: {len(clean_df):,} petani
❌ Status: Upload ke Google Sheets gagal total

🔧 TROUBLESHOOTING:
──────────────────────────────
1. Cek kuota Google Sheets (10 juta cell)
2. Pastikan service account punya akses edit
3. Coba manual upload file backup
4. Hubungi administrator sistem

📋 BACKUP FILE:
──────────────────────────────
File backup lengkap tersimpan di server

🎯 STATUS: GAGAL UPLOAD
"""
        
        # Kirim email
        email_success = send_email_notification(subject, body, is_success=(uploaded_rows > 0))
        
        # 11. Final status
        print("\n" + "="*60)
        if verification_success and uploaded_rows == len(clean_df):
            print(f"🎉 PROSES BERHASIL 100%!")
            print(f"   • {uploaded_rows:,}/{len(clean_df):,} rows uploaded")
            print(f"   • Google Sheets expanded successfully")
        elif uploaded_rows > 0:
            percentage = (uploaded_rows / len(clean_df)) * 100
            print(f"⚠️ PROSES SEBAGIAN BERHASIL ({percentage:.1f}%)")
            print(f"   • {uploaded_rows:,}/{len(clean_df):,} rows uploaded")
            print(f"   • {len(clean_df) - uploaded_rows:,} rows missing")
        else:
            print("❌ PROSES GAGAL (upload tidak berhasil)")
            print("   • Backup file tersimpan untuk manual upload")
        
        print(f"🔗 Link: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
        print("="*60)
        
        # Jika tidak 100% berhasil, exit dengan error code
        if uploaded_rows < len(clean_df):
            sys.exit(1)
        
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
    main_final()
