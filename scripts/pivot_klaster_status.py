"""
pivot_klaster_status.py - VERSI SUPER KETAT
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
from datetime import datetime, date
import traceback
import json
import time
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ============================
# KONFIGURASI
# ============================
FOLDER_ID = "1D2_eMQ28MadcGDKWn9lmVd-50ZnqNQMn"
KECAMATAN_SHEET_URL = "https://docs.google.com/spreadsheets/d/1doC0t-ni1up79sxAIB_uxT9yNlNm5u4FetGbIgLtiK8/edit"
KIOS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1R5ok4B-0AAlZd3gblMViRrlD7hGw4hchynS4tT2d0gc/edit"

MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 30
WRITE_DELAY = 5

HEADER_FORMAT = {
    "backgroundColor": {"red": 0.0, "green": 0.3, "blue": 0.6},
    "textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
    "horizontalAlignment": "CENTER"
}

# ============================
# FUNGSI KLASIFIKASI STATUS - VERSI SUPER KETAT
# ============================
def klasifikasikan_status(status_value):
    """
    Klasifikasi status dengan logika SUPER KETAT:
    1. Jika status mengandung "Disetujui" -> selalu DISETUJUI, abaikan "menunggu" dalam kurung
    2. Hanya status "Menunggu verifikasi tim verval kecamatan" yang jadi MENUNGGU_KEC
    3. Pastikan tidak ada ambiguitas
    """
    if pd.isna(status_value) or status_value is None:
        return "TANPA_STATUS"
    
    status_str = str(status_value).strip()
    status_lower = status_str.lower()
    
    # **DEBUG: Tampilkan status asli**
    debug_info = []
    debug_info.append(f"Status asli: '{status_str}'")
    
    # **STEP 1: Cek apakah ini benar-benar status MENUNGGU murni**
    # HANYA jika statusnya persis seperti ini (atau variasi kecil):
    # "Menunggu verifikasi tim verval kecamatan"
    
    # Pattern untuk MENUNGGU_KEC yang ketat
    menunggu_kec_patterns = [
        'menunggu verifikasi tim verval kecamatan',
        'menunggu verifikasi kecamatan',
        'menunggu verval kecamatan',
        'menunggu kecamatan',
    ]
    
    is_menunggu_kec = False
    for pattern in menunggu_kec_patterns:
        if pattern in status_lower:
            is_menunggu_kec = True
            debug_info.append(f"✓ Match pattern MENUNGGU_KEC: '{pattern}'")
            break
    
    if is_menunggu_kec:
        # Tapi harus TIDAK mengandung kata "disetujui" sama sekali
        if 'disetujui' not in status_lower:
            debug_info.append(f"✅ Klasifikasi: MENUNGGU_KEC (tidak ada 'disetujui')")
            print("\n".join(debug_info))
            return "MENUNGGU_KEC"
        else:
            debug_info.append(f"⚠️  Skip MENUNGGU_KEC karena ada 'disetujui'")
    
    # **STEP 2: Cek DISETUJUI (prioritas utama)**
    # Jika ada kata "Disetujui" di mana saja dalam status
    if 'disetujui' in status_lower:
        debug_info.append(f"✓ Kata kunci 'disetujui' ditemukan")
        
        # **HAPUS SEMUA KONTEN DALAM KURUNG** sebelum cek pusat/kecamatan
        import re
        status_no_brackets = re.sub(r'\(.*?\)', '', status_lower)
        status_no_brackets = re.sub(r'\s+', ' ', status_no_brackets).strip()
        
        debug_info.append(f"  Setelah hapus kurung: '{status_no_brackets}'")
        
        # Cek apakah mengandung "pusat" atau "kecamatan" di teks tanpa kurung
        if 'pusat' in status_no_brackets:
            debug_info.append(f"✅ Klasifikasi: DISETUJUI_PUSAT (ada 'pusat' di teks utama)")
            print("\n".join(debug_info))
            return "DISETUJUI_PUSAT"
        elif 'kecamatan' in status_no_brackets:
            debug_info.append(f"✅ Klasifikasi: DISETUJUI_KEC (ada 'kecamatan' di teks utama)")
            print("\n".join(debug_info))
            return "DISETUJUI_KEC"
        else:
            # Fallback: cek di string lengkap
            if 'pusat' in status_lower:
                debug_info.append(f"✅ Klasifikasi: DISETUJUI_PUSAT (ada 'pusat' di string lengkap)")
                print("\n".join(debug_info))
                return "DISETUJUI_PUSAT"
            elif 'kecamatan' in status_lower:
                debug_info.append(f"✅ Klasifikasi: DISETUJUI_KEC (ada 'kecamatan' di string lengkap)")
                print("\n".join(debug_info))
                return "DISETUJUI_KEC"
            else:
                debug_info.append(f"⚠️  Klasifikasi: DISETUJUI_LAIN (tidak ada pusat/kecamatan)")
                print("\n".join(debug_info))
                return "DISETUJUI_LAIN"
    
    # **STEP 3: Cek DITOLAK**
    elif 'ditolak' in status_lower:
        if 'pusat' in status_lower:
            return "DITOLAK_PUSAT"
        elif 'kecamatan' in status_lower:
            return "DITOLAK_KEC"
        else:
            return "DITOLAK_LAIN"
    
    # **STEP 4: Cek MENUNGGU lainnya (selain KEC)**
    elif 'menunggu' in status_lower:
        if 'pusat' in status_lower:
            return "MENUNGGU_PUSAT"
        else:
            return "MENUNGGU_LAIN"
    
    # **STEP 5: Default**
    debug_info.append(f"⚠️  Klasifikasi: LAINNYA (tidak match)")
    print("\n".join(debug_info))
    return "LAINNYA"

# Fungsi testing untuk memverifikasi
def test_klasifikasi():
    """Test fungsi klasifikasi dengan contoh-contoh"""
    test_cases = [
        ("Menunggu verifikasi tim verval kecamatan", "MENUNGGU_KEC"),
        ("menunggu verifikasi kecamatan", "MENUNGGU_KEC"),
        ("Menunggu verval kecamatan", "MENUNGGU_KEC"),
        ("Disetujui tim verval kecamatan", "DISETUJUI_KEC"),
        ("Disetujui tim verval kecamatan (menunggu verifikasi tim verval pusat)", "DISETUJUI_KEC"),
        ("disetujui kecamatan (menunggu)", "DISETUJUI_KEC"),
        ("Disetujui tim verval pusat", "DISETUJUI_PUSAT"),
        ("disetujui pusat", "DISETUJUI_PUSAT"),
        ("Disetujui tim verval pusat (verifikasi)", "DISETUJUI_PUSAT"),
        ("Ditolak tim verval kecamatan", "DITOLAK_KEC"),
        ("Ditolak tim verval pusat", "DITOLAK_PUSAT"),
        ("Menunggu verifikasi tim verval pusat", "MENUNGGU_PUSAT"),
        ("Status lainnya", "LAINNYA"),
        ("", "TANPA_STATUS"),
        (None, "TANPA_STATUS"),
    ]
    
    print("🧪 TESTING FUNGSI KLASIFIKASI:")
    print("=" * 80)
    
    all_passed = True
    for i, (input_status, expected) in enumerate(test_cases):
        result = klasifikasikan_status(input_status)
        passed = result == expected
        all_passed = all_passed and passed
        
        status = "✅" if passed else "❌"
        print(f"{status} Test {i+1}: '{input_status}'")
        print(f"   Expected: {expected}")
        print(f"   Got:      {result}")
        if not passed:
            print(f"   ❌ MISMATCH!")
        print()
    
    print(f"📊 Hasil: {'SEMUA TEST BERHASIL ✅' if all_passed else 'ADA TEST YANG GAGAL ❌'}")
    return all_passed

def get_klaster_display_name(klaster):
    mapping = {
        "DISETUJUI_PUSAT": "Setuju_Pusat",
        "DISETUJUI_KEC": "Setuju_Kec",
        "MENUNGGU_KEC": "Menunggu_Kec",
        "MENUNGGU_PUSAT": "Menunggu_Pusat",
        "DITOLAK_PUSAT": "Tolak_Pusat",
        "DITOLAK_KEC": "Tolak_Kec",
        "DITOLAK_LAIN": "Tolak_Lain",
        "MENUNGGU_LAIN": "Menunggu_Lain",
        "DISETUJUI_LAIN": "Setuju_Lain",
        "TANPA_STATUS": "No_Status",
        "LAINNYA": "Lainnya"
    }
    return mapping.get(klaster, klaster)

# ============================
# FUNGSI UTAMA DENGAN DEBUGGING MENDALAM
# ============================
def process_verval_pupuk_by_klaster():
    print("=" * 80)
    print("🚀 PROSES REKAP DATA - VERSI SUPER KETAT")
    print("=" * 80)
    
    # Test fungsi klasifikasi dulu
    print("\n🔬 TESTING FUNGSI KLASIFIKASI:")
    if not test_klasifikasi():
        print("❌ Fungsi klasifikasi gagal test!")
        return
    
    try:
        # Load credentials
        creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON tidak ditemukan")

        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )

        gc = gspread.authorize(credentials)

        # Download files
        def download_excel_files():
            os.makedirs("data_excel", exist_ok=True)
            drive_service = build('drive', 'v3', credentials=credentials)
            query = f"'{FOLDER_ID}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
            results = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            if not files:
                raise ValueError("❌ Tidak ada file Excel di folder Google Drive.")

            paths = []
            for file in files:
                print(f"📥 Downloading: {file['name']}")
                request = drive_service.files().get_media(fileId=file["id"])
                safe_filename = "".join(c for c in file['name'] if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
                file_path = os.path.join("data_excel", safe_filename)

                with io.FileIO(file_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()

                paths.append({'path': file_path, 'name': file['name'], 'id': file['id']})

            print(f"✅ Berhasil download {len(paths)} file Excel")
            return paths

        excel_files = download_excel_files()
        
        expected_columns = ['KECAMATAN', 'NO TRANSAKSI', 'KODE KIOS', 'NAMA KIOS', 'NIK', 'NAMA PETANI',
                          'UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR',
                          'TGL TEBUS', 'STATUS']
        
        pupuk_columns = ['UREA', 'NPK', 'SP36', 'ZA', 'NPK FORMULA', 'ORGANIK', 'ORGANIK CAIR']

        all_data = []
        
        print("\n" + "=" * 80)
        print("🔍 DEBUG DETAIL: ANALISIS STATUS PER FILE")
        print("=" * 80)

        for file_info in excel_files:
            file_path = file_info['path']
            file_name = file_info['name']

            print(f"\n📖 File: {file_name}")

            try:
                df = pd.read_excel(file_path, sheet_name='Worksheet')

                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"   ⚠️  Missing columns: {missing_columns}")
                    continue

                # Clean data
                df['NIK'] = df['NIK'].apply(lambda x: re.sub(r'\D', '', str(x)) if pd.notna(x) else None)
                df = df[df['NIK'].notna()]

                for col in pupuk_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # **ANALISIS MENDALAM: Status dalam file ini**
                if 'STATUS' in df.columns:
                    print(f"   📊 Analisis status dalam {file_name}:")
                    
                    # Terapkan klasifikasi
                    df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(klasifikasikan_status)
                    
                    # Hitung distribusi
                    status_counts = df['KLASIFIKASI_STATUS'].value_counts()
                    
                    # Tampilkan khusus untuk MENUNGGU_KEC
                    if "MENUNGGU_KEC" in status_counts:
                        count_menunggu_kec = status_counts["MENUNGGU_KEC"]
                        print(f"   ⚠️  ⚠️  ⚠️  PERHATIAN: Ditemukan {count_menunggu_kec} data MENUNGGU_KEC!")
                        
                        # Tampilkan semua status yang jadi MENUNGGU_KEC
                        menunggu_kec_data = df[df['KLASIFIKASI_STATUS'] == "MENUNGGU_KEC"]
                        unique_statuses = menunggu_kec_data['STATUS'].dropna().unique()
                        
                        print(f"   🔍 Status yang diklasifikasikan sebagai MENUNGGU_KEC:")
                        for i, status in enumerate(unique_statuses):
                            print(f"      {i+1}. '{status}'")
                        
                        # Simpan ke file untuk analisis lebih lanjut
                        debug_file = f"debug_menunggu_kec_{file_name}.csv"
                        menunggu_kec_data[['STATUS', 'KLASIFIKASI_STATUS']].to_csv(debug_file, index=False)
                        print(f"   💾 Data debug disimpan ke: {debug_file}")
                    
                    # Tampilkan semua distribusi
                    for status, count in status_counts.items():
                        print(f"      • {status}: {count} data")
                
                all_data.append(df)
                print(f"   ✅ Berhasil: {len(df)} baris")

            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
                continue

        if not all_data:
            error_msg = "Tidak ada data yang berhasil diproses!"
            print(f"❌ {error_msg}")
            return

        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Total data gabungan: {len(combined_df):,} baris")
        
        # **ANALISIS FINAL: Distribusi klasifikasi**
        print("\n" + "=" * 80)
        print("📈 DISTRIBUSI FINAL SETELAH KLASIFIKASI")
        print("=" * 80)
        
        # Pastikan kolom klasifikasi ada
        if 'KLASIFIKASI_STATUS' not in combined_df.columns:
            print("   ⚠️  Membuat kolom KLASIFIKASI_STATUS...")
            combined_df['KLASIFIKASI_STATUS'] = combined_df['STATUS'].apply(klasifikasikan_status)
        
        status_counts = combined_df['KLASIFIKASI_STATUS'].value_counts()
        total_data = len(combined_df)
        
        print(f"\n📊 TOTAL DATA: {total_data:,}")
        print(f"📝 DISTRIBUSI KLASTER:")
        
        for status, count in status_counts.items():
            percentage = (count / total_data) * 100
            display_name = get_klaster_display_name(status)
            print(f"   • {display_name}: {count:,} data ({percentage:.1f}%)")
        
        # **ANALISIS KHUSUS: Jika masih ada MENUNGGU_KEC**
        if "MENUNGGU_KEC" in status_counts and status_counts["MENUNGGU_KEC"] > 0:
            print(f"\n⚠️  ⚠️  ⚠️  MASALAH TERDETEKSI!")
            print(f"   Masih ada {status_counts['MENUNGGU_KEC']:,} data MENUNGGU_KEC")
            
            # Analisis detail
            menunggu_kec_data = combined_df[combined_df['KLASIFIKASI_STATUS'] == "MENUNGGU_KEC"]
            
            print(f"\n🔍 ANALISIS DATA MENUNGGU_KEC:")
            print(f"   Total: {len(menunggu_kec_data):,} data")
            
            # Group by status asli
            status_groups = menunggu_kec_data['STATUS'].value_counts()
            print(f"\n   📋 KELOMPOK STATUS YANG JADI MENUNGGU_KEC:")
            for status, count in status_groups.items():
                print(f"      • '{status}': {count:,} data")
            
            # Simpan untuk analisis
            debug_file = "debug_all_menunggu_kec.csv"
            menunggu_kec_data[['STATUS', 'KLASIFIKASI_STATUS']].to_csv(debug_file, index=False)
            print(f"\n   💾 Data debug lengkap disimpan ke: {debug_file}")
            
            # Tanya user apakah ingin melanjutkan
            print(f"\n❓ LANJUTKAN PROSES? (data MENUNGGU_KEC akan tetap ada)")
            print(f"   Tekan Enter untuk melanjutkan, Ctrl+C untuk membatalkan...")
            try:
                input()
            except:
                print("Proses dibatalkan")
                return
        
        # **Bersihkan semua sheet lama**
        print(f"\n🗑️  MEMBERSIHKAN SHEET LAMA...")
        
        def clear_spreadsheet(url):
            try:
                spreadsheet = gc.open_by_url(url)
                sheets = spreadsheet.worksheets()
                for sheet in sheets:
                    if sheet.title != "Sheet1":
                        spreadsheet.del_worksheet(sheet)
                        print(f"   ✅ Menghapus: {sheet.title}")
                        time.sleep(WRITE_DELAY)
            except Exception as e:
                print(f"   ⚠️  Gagal clear {url}: {str(e)}")
        
        clear_spreadsheet(KECAMATAN_SHEET_URL)
        clear_spreadsheet(KIOS_SHEET_URL)
        
        # **Buat pivot kecamatan**
        print(f"\n📊 MEMBUAT PIVOT KECAMATAN...")
        def create_pivot_kecamatan(df):
            pivots = {}
            
            for klaster in df['KLASIFIKASI_STATUS'].unique():
                df_klaster = df[df['KLASIFIKASI_STATUS'] == klaster].copy()
                
                if len(df_klaster) > 0:
                    pivot = df_klaster.groupby('KECAMATAN')[pupuk_columns].sum().reset_index()
                    
                    # Add total row
                    total_row = {col: pivot[col].sum() for col in pupuk_columns}
                    total_row['KECAMATAN'] = "TOTAL"
                    for col in pivot.columns:
                        if col not in pupuk_columns and col != 'KECAMATAN':
                            total_row[col] = ""
                    
                    total_df = pd.DataFrame([total_row])
                    pivot = pd.concat([pivot, total_df], ignore_index=True)
                    
                    for col in pupuk_columns:
                        if col in pivot.columns:
                            pivot[col] = pivot[col].round(2)
                    
                    pivots[klaster] = pivot
            
            return pivots
        
        # **Buat pivot kios**
        print(f"\n📊 MEMBUAT PIVOT KIOS...")
        def create_pivot_kios(df):
            pivots = {}
            
            for klaster in df['KLASIFIKASI_STATUS'].unique():
                df_klaster = df[df['KLASIFIKASI_STATUS'] == klaster].copy()
                
                if len(df_klaster) > 0:
                    pivot = df_klaster.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[pupuk_columns].sum().reset_index()
                    pivot = pivot[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + pupuk_columns]
                    
                    # Add total row
                    total_row = {col: pivot[col].sum() for col in pupuk_columns}
                    total_row['KECAMATAN'] = "TOTAL"
                    total_row['KODE KIOS'] = ""
                    total_row['NAMA KIOS'] = ""
                    for col in pivot.columns:
                        if col not in pupuk_columns and col not in ['KECAMATAN', 'KODE KIOS', 'NAMA KIOS']:
                            total_row[col] = ""
                    
                    total_df = pd.DataFrame([total_row])
                    pivot = pd.concat([pivot, total_df], ignore_index=True)
                    
                    for col in pupuk_columns:
                        if col in pivot.columns:
                            pivot[col] = pivot[col].round(2)
                    
                    pivots[klaster] = pivot
            
            return pivots
        
        # **Upload ke Google Sheets**
        def upload_to_sheets(pivots, spreadsheet_url, sheet_type):
            print(f"\n📤 UPLOADING {sheet_type} PIVOTS...")
            
            spreadsheet = gc.open_by_url(spreadsheet_url)
            
            sheet_count = 0
            for klaster, pivot_df in pivots.items():
                sheet_name = get_klaster_display_name(klaster)
                
                print(f"   📝 {sheet_name}: {len(pivot_df)-1} baris data")
                
                try:
                    # Create worksheet
                    worksheet = spreadsheet.add_worksheet(
                        title=sheet_name,
                        rows=str(len(pivot_df) + 10),
                        cols=str(len(pivot_df.columns) + 5)
                    )
                    
                    # Upload data
                    worksheet.update(
                        [pivot_df.columns.values.tolist()] + pivot_df.values.tolist()
                    )
                    
                    # Apply header format
                    worksheet.format('A1:Z1', HEADER_FORMAT)
                    
                    sheet_count += 1
                    time.sleep(WRITE_DELAY)
                    
                except Exception as e:
                    print(f"   ❌ Gagal upload {sheet_name}: {str(e)}")
            
            print(f"✅ {sheet_type} sheets dibuat: {sheet_count}")
            return sheet_count
        
        # Process and upload
        kecamatan_pivots = create_pivot_kecamatan(combined_df)
        kecamatan_sheet_count = upload_to_sheets(kecamatan_pivots, KECAMATAN_SHEET_URL, "KECAMATAN")
        
        kios_pivots = create_pivot_kios(combined_df)
        kios_sheet_count = upload_to_sheets(kios_pivots, KIOS_SHEET_URL, "KIOS")
        
        # **Kirim email notifikasi**
        print(f"\n📧 MENYIAPKAN NOTIFIKASI EMAIL...")
        
        success_message = f"""
REKAP DATA BERDASARKAN KLASTER STATUS BERHASIL ✓

📊 STATISTIK UMUM:
• File diproses: {len(excel_files)}
• Total data: {len(combined_df):,} baris
• Sheet Kecamatan: {kecamatan_sheet_count} klaster
• Sheet Kios: {kios_sheet_count} klaster

📋 DISTRIBUSI STATUS:
"""
        for status, count in status_counts.items():
            percentage = (count / total_data) * 100
            display_name = get_klaster_display_name(status)
            success_message += f"• {display_name}: {count:,} data ({percentage:.1f}%)\n"
        
        # Tambahkan warning jika masih ada MENUNGGU_KEC
        if "MENUNGGU_KEC" in status_counts and status_counts["MENUNGGU_KEC"] > 0:
            success_message += f"""
⚠️  PERHATIAN:
• Masih ditemukan {status_counts['MENUNGGU_KEC']:,} data MENUNGGU_KEC
• File debug telah disimpan untuk analisis
• Periksa file debug_all_menunggu_kec.csv untuk detail
"""
        
        success_message += f"""
🔗 LINK HASIL:
• Pivot Kecamatan: {KECAMATAN_SHEET_URL}
• Pivot Kios: {KIOS_SHEET_URL}
"""
        
        # Load email config and send
        try:
            EMAIL_CONFIG = {
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "sender_email": os.getenv("SENDER_EMAIL"),
                "sender_password": os.getenv("SENDER_EMAIL_PASSWORD"),
                "recipient_emails": json.loads(os.getenv("RECIPIENT_EMAILS")),
            }
            
            msg = MIMEMultipart()
            msg['From'] = EMAIL_CONFIG["sender_email"]
            msg['To'] = ", ".join(EMAIL_CONFIG["recipient_emails"])
            msg['Subject'] = f"[verval-pupuk2] REKAP KLASTER BERHASIL"
            
            email_body = f"""
            <html>
                <body>
                    <h2 style="color: green;">✅ REKAP KLASTER BERHASIL</h2>
                    <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                        {success_message.replace(chr(10), '<br>')}
                    </div>
                </body>
            </html>
            """
            
            msg.attach(MIMEText(email_body, 'html'))
            
            with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
                server.starttls()
                server.login(EMAIL_CONFIG["sender_email"], EMAIL_CONFIG["sender_password"])
                server.send_message(msg)
            
            print(f"✅ Email notifikasi terkirim")
            
        except Exception as e:
            print(f"⚠️  Gagal kirim email: {str(e)}")
        
        print("\n" + "=" * 80)
        print("✅ PROSES SELESAI!")
        print("=" * 80)
        
        # **Tampilkan summary akhir**
        print(f"\n📋 SUMMARY AKHIR:")
        print(f"   • Total data: {len(combined_df):,}")
        print(f"   • MENUNGGU_KEC: {status_counts.get('MENUNGGU_KEC', 0):,}")
        print(f"   • DISETUJUI_KEC: {status_counts.get('DISETUJUI_KEC', 0):,}")
        print(f"   • DISETUJUI_PUSAT: {status_counts.get('DISETUJUI_PUSAT', 0):,}")
        
        if status_counts.get('MENUNGGU_KEC', 0) > 0:
            print(f"\n⚠️  REKOMENDASI:")
            print(f"   1. Periksa file debug_all_menunggu_kec.csv")
            print(f"   2. Perbaiki fungsi klasifikasi berdasarkan data debug")
            print(f"   3. Jalankan ulang script setelah perbaikan")

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print(traceback.format_exc())

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    process_verval_pupuk_by_klaster()
