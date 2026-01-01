"""
pivot_klaster_status.py - VERSI FINAL DENGAN FORCE FIX
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
# FUNGSI KLASIFIKASI STATUS - VERSI FINAL DENGAN FORCE FIX
# ============================
def klasifikasikan_status(status_value):
    """
    Klasifikasi status dengan FORCE FIX:
    1. Jika ada "disetujui" -> selalu DISETUJUI (abaikan "menunggu")
    2. HANYA benar-benar murni "Menunggu verifikasi tim verval kecamatan" yang jadi MENUNGGU_KEC
    3. Semua "menunggu" lainnya masuk ke MENUNGGU_LAIN
    """
    if pd.isna(status_value) or status_value is None or str(status_value).strip() == '':
        return "TANPA_STATUS"
    
    status_str = str(status_value).strip()
    status_lower = status_str.lower()
    
    # **STEP 1: Cek DISETUJUI terlebih dahulu (PRIORITAS TERTINGGI)**
    # Jika ada kata "disetujui" di status manapun, selalu prioritaskan sebagai DISETUJUI
    if 'disetujui' in status_lower:
        # **FORCE FIX: Abaikan semua kata "menunggu" jika ada "disetujui"**
        # Hapus semua konten dalam kurung untuk pengecekan
        import re
        status_no_brackets = re.sub(r'\(.*?\)', '', status_lower)
        status_no_brackets = re.sub(r'\s+', ' ', status_no_brackets).strip()
        
        # Cek pusat/kecamatan di teks tanpa kurung
        if 'pusat' in status_no_brackets:
            return "DISETUJUI_PUSAT"
        elif 'kecamatan' in status_no_brackets:
            return "DISETUJUI_KEC"
        else:
            # Fallback ke string lengkap
            if 'pusat' in status_lower:
                return "DISETUJUI_PUSAT"
            elif 'kecamatan' in status_lower:
                return "DISETUJUI_KEC"
            else:
                return "DISETUJUI_LAIN"
    
    # **STEP 2: Cek MENUNGGU (HANYA jika tidak ada "disetujui" sama sekali)**
    elif 'menunggu' in status_lower:
        # **KETAT: Hanya status tertentu yang boleh jadi MENUNGGU_KEC**
        # Pattern yang diizinkan untuk MENUNGGU_KEC:
        allowed_menunggu_kec = [
            'menunggu verifikasi tim verval kecamatan',
            'menunggu verifikasi verval kecamatan',
            'menunggu verifikasi kecamatan',
        ]
        
        is_allowed = False
        for pattern in allowed_menunggu_kec:
            if pattern in status_lower:
                is_allowed = True
                break
        
        if is_allowed:
            return "MENUNGGU_KEC"
        elif 'pusat' in status_lower:
            return "MENUNGGU_PUSAT"
        else:
            return "MENUNGGU_LAIN"
    
    # **STEP 3: Cek DITOLAK**
    elif 'ditolak' in status_lower:
        if 'pusat' in status_lower:
            return "DITOLAK_PUSAT"
        elif 'kecamatan' in status_lower:
            return "DITOLAK_KEC"
        else:
            return "DITOLAK_LAIN"
    
    # **STEP 4: Default**
    else:
        return "LAINNYA"

# Fungsi ALTERNATIF untuk FORCE CONVERT semua ke DISETUJUI
def force_convert_status(status_value):
    """
    FORCE CONVERT: Ubah semua status yang mengandung kata kunci tertentu
    """
    if pd.isna(status_value) or status_value is None or str(status_value).strip() == '':
        return "TANPA_STATUS"
    
    status_str = str(status_value).strip()
    status_lower = status_str.lower()
    
    # **FORCE LOGIC:**
    # 1. Jika mengandung "disetujui" dan "kecamatan" -> DISETUJUI_KEC
    # 2. Jika mengandung "disetujui" dan "pusat" -> DISETUJUI_PUSAT
    # 3. Jika mengandung "menunggu" dan "kecamatan" -> DISETUJUI_KEC (FORCE!)
    # 4. Jika mengandung "menunggu" dan "pusat" -> DISETUJUI_PUSAT (FORCE!)
    # 5. Default ke DISETUJUI_LAIN
    
    # Cek kata kunci
    has_disetujui = 'disetujui' in status_lower
    has_menunggu = 'menunggu' in status_lower
    has_kecamatan = 'kecamatan' in status_lower
    has_pusat = 'pusat' in status_lower
    
    # FORCE CONVERT: Semua "menunggu" yang ada "kecamatan" jadi DISETUJUI_KEC
    if has_menunggu and has_kecamatan:
        return "DISETUJUI_KEC"
    
    # FORCE CONVERT: Semua "menunggu" yang ada "pusat" jadi DISETUJUI_PUSAT
    if has_menunggu and has_pusat:
        return "DISETUJUI_PUSAT"
    
    # Normal logic untuk "disetujui"
    if has_disetujui:
        if has_pusat:
            return "DISETUJUI_PUSAT"
        elif has_kecamatan:
            return "DISETUJUI_KEC"
        else:
            return "DISETUJUI_LAIN"
    
    # Untuk "ditolak"
    if 'ditolak' in status_lower:
        if has_pusat:
            return "DITOLAK_PUSAT"
        elif has_kecamatan:
            return "DITOLAK_KEC"
        else:
            return "DITOLAK_LAIN"
    
    return "LAINNYA"

# ============================
# FUNGSI UTAMA DENGAN OPSI FORCE CONVERT
# ============================
def process_verval_pupuk_with_force_fix(use_force_convert=False):
    """
    Proses data dengan opsi FORCE CONVERT
    use_force_convert=True: Gunakan force_convert_status()
    use_force_convert=False: Gunakan klasifikasikan_status()
    """
    print("=" * 80)
    print("🚀 PROSES REKAP DATA DENGAN FORCE FIX" if use_force_convert else "🚀 PROSES REKAP DATA NORMAL")
    print("=" * 80)
    
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
        print("📊 ANALISIS STATUS SEBELUM DAN SESUDAH KLASIFIKASI")
        print("=" * 80)

        for file_info in excel_files:
            file_path = file_info['path']
            file_name = file_info['name']

            print(f"\n📖 File: {file_name}")

            try:
                df = pd.read_excel(file_path, sheet_name='Worksheet')

                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"   ⚠️  Missing: {missing_columns}")
                    continue

                # Clean data
                df['NIK'] = df['NIK'].apply(lambda x: re.sub(r'\D', '', str(x)) if pd.notna(x) else None)
                df = df[df['NIK'].notna()]

                for col in pupuk_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # **ANALISIS STATUS ASLI**
                if 'STATUS' in df.columns:
                    # Analisis sebelum klasifikasi
                    print(f"   🔍 Status unik dalam file:")
                    status_counts_raw = df['STATUS'].value_counts()
                    
                    for status, count in status_counts_raw.head(10).items():
                        print(f"      • '{status}': {count} data")
                    
                    # Terapkan klasifikasi (pilih fungsi berdasarkan parameter)
                    if use_force_convert:
                        df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(force_convert_status)
                        print(f"   ⚡ MENGGUNAKAN FORCE CONVERT")
                    else:
                        df['KLASIFIKASI_STATUS'] = df['STATUS'].apply(klasifikasikan_status)
                    
                    # Analisis setelah klasifikasi
                    status_counts_classified = df['KLASIFIKASI_STATUS'].value_counts()
                    
                    print(f"   📈 Setelah klasifikasi:")
                    for status, count in status_counts_classified.items():
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
        
        # **ANALISIS FINAL**
        print("\n" + "=" * 80)
        print("📈 DISTRIBUSI FINAL")
        print("=" * 80)
        
        if 'KLASIFIKASI_STATUS' not in combined_df.columns:
            if use_force_convert:
                combined_df['KLASIFIKASI_STATUS'] = combined_df['STATUS'].apply(force_convert_status)
            else:
                combined_df['KLASIFIKASI_STATUS'] = combined_df['STATUS'].apply(klasifikasikan_status)
        
        status_counts = combined_df['KLASIFIKASI_STATUS'].value_counts()
        total_data = len(combined_df)
        
        print(f"\n📊 TOTAL DATA: {total_data:,}")
        print(f"📝 DISTRIBUSI AKHIR:")
        
        for status, count in status_counts.items():
            percentage = (count / total_data) * 100
            print(f"   • {status}: {count:,} data ({percentage:.1f}%)")
        
        # **ANALISIS DETAIL untuk MENUNGGU_KEC**
        if "MENUNGGU_KEC" in status_counts and status_counts["MENUNGGU_KEC"] > 0:
            print(f"\n⚠️  MASIH ADA DATA MENUNGGU_KEC: {status_counts['MENUNGGU_KEC']:,}")
            
            menunggu_kec_data = combined_df[combined_df['KLASIFIKASI_STATUS'] == "MENUNGGU_KEC"]
            
            print(f"🔍 Status yang masih jadi MENUNGGU_KEC:")
            status_groups = menunggu_kec_data['STATUS'].value_counts().head(20)
            
            for status, count in status_groups.items():
                print(f"   • '{status}': {count:,} data")
            
            # Tanya user apakah ingin force convert
            if not use_force_convert:
                print(f"\n❓ INGIN FORCE CONVERT SEMUA KE DISETUJUI?")
                print(f"   Tekan Enter untuk force convert, Ctrl+C untuk lanjut tanpa convert...")
                try:
                    input()
                    
                    # Force convert
                    print(f"⚡ MELAKUKAN FORCE CONVERT...")
                    combined_df['KLASIFIKASI_STATUS'] = combined_df['STATUS'].apply(force_convert_status)
                    
                    # Update counts
                    status_counts = combined_df['KLASIFIKASI_STATUS'].value_counts()
                    print(f"\n📊 SETELAH FORCE CONVERT:")
                    for status, count in status_counts.items():
                        percentage = (count / total_data) * 100
                        print(f"   • {status}: {count:,} data ({percentage:.1f}%)")
                        
                except KeyboardInterrupt:
                    print(f"   Melanjutkan tanpa force convert")
        
        # **Bersihkan sheet lama dan upload**
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
        
        # **Buat dan upload pivot**
        print(f"\n📊 MEMBUAT DAN UPLOAD PIVOT...")
        
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
        
        def create_and_upload_pivots():
            # Kecamatan pivot
            kecamatan_sheets = 0
            kios_sheets = 0
            
            for sheet_type, url in [("KECAMATAN", KECAMATAN_SHEET_URL), ("KIOS", KIOS_SHEET_URL)]:
                print(f"\n📤 UPLOADING {sheet_type} PIVOTS...")
                
                spreadsheet = gc.open_by_url(url)
                
                for klaster in combined_df['KLASIFIKASI_STATUS'].unique():
                    df_klaster = combined_df[combined_df['KLASIFIKASI_STATUS'] == klaster].copy()
                    
                    if len(df_klaster) > 0:
                        if sheet_type == "KECAMATAN":
                            # Group by kecamatan
                            pivot = df_klaster.groupby('KECAMATAN')[pupuk_columns].sum().reset_index()
                            
                            # Add total
                            total_row = {col: pivot[col].sum() for col in pupuk_columns}
                            total_row['KECAMATAN'] = "TOTAL"
                            for col in pivot.columns:
                                if col not in pupuk_columns and col != 'KECAMATAN':
                                    total_row[col] = ""
                            
                            total_df = pd.DataFrame([total_row])
                            pivot = pd.concat([pivot, total_df], ignore_index=True)
                            
                        else:  # KIOS
                            # Group by kecamatan, kode kios, nama kios
                            pivot = df_klaster.groupby(['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'])[pupuk_columns].sum().reset_index()
                            pivot = pivot[['KECAMATAN', 'KODE KIOS', 'NAMA KIOS'] + pupuk_columns]
                            
                            # Add total
                            total_row = {col: pivot[col].sum() for col in pupuk_columns}
                            total_row['KECAMATAN'] = "TOTAL"
                            total_row['KODE KIOS'] = ""
                            total_row['NAMA KIOS'] = ""
                            for col in pivot.columns:
                                if col not in pupuk_columns and col not in ['KECAMATAN', 'KODE KIOS', 'NAMA KIOS']:
                                    total_row[col] = ""
                            
                            total_df = pd.DataFrame([total_row])
                            pivot = pd.concat([pivot, total_df], ignore_index=True)
                        
                        # Format numeric
                        for col in pupuk_columns:
                            if col in pivot.columns:
                                pivot[col] = pivot[col].round(2)
                        
                        # Upload
                        sheet_name = get_klaster_display_name(klaster)
                        print(f"   📝 {sheet_name}: {len(pivot)-1} baris")
                        
                        try:
                            worksheet = spreadsheet.add_worksheet(
                                title=sheet_name,
                                rows=str(len(pivot) + 10),
                                cols=str(len(pivot.columns) + 5)
                            )
                            
                            worksheet.update(
                                [pivot.columns.values.tolist()] + pivot.values.tolist()
                            )
                            
                            worksheet.format('A1:Z1', HEADER_FORMAT)
                            
                            if sheet_type == "KECAMATAN":
                                kecamatan_sheets += 1
                            else:
                                kios_sheets += 1
                                
                            time.sleep(WRITE_DELAY)
                            
                        except Exception as e:
                            print(f"   ❌ Gagal upload {sheet_name}: {str(e)}")
            
            return kecamatan_sheets, kios_sheets
        
        kecamatan_sheet_count, kios_sheet_count = create_and_upload_pivots()
        
        # **Summary akhir**
        print("\n" + "=" * 80)
        print("✅ PROSES SELESAI!")
        print("=" * 80)
        
        print(f"\n📋 SUMMARY AKHIR:")
        print(f"   • Total data: {len(combined_df):,}")
        print(f"   • MENUNGGU_KEC: {status_counts.get('MENUNGGU_KEC', 0):,}")
        print(f"   • DISETUJUI_KEC: {status_counts.get('DISETUJUI_KEC', 0):,}")
        print(f"   • DISETUJUI_PUSAT: {status_counts.get('DISETUJUI_PUSAT', 0):,}")
        print(f"   • Sheet Kecamatan: {kecamatan_sheet_count}")
        print(f"   • Sheet Kios: {kios_sheet_count}")
        
        # **Kirim email**
        try:
            # Load email config
            sender_email = os.getenv("SENDER_EMAIL")
            sender_password = os.getenv("SENDER_EMAIL_PASSWORD")
            recipient_emails = json.loads(os.getenv("RECIPIENT_EMAILS"))
            
            if all([sender_email, sender_password, recipient_emails]):
                msg = MIMEMultipart()
                msg['From'] = sender_email
                msg['To'] = ", ".join(recipient_emails)
                
                mode = "DENGAN FORCE CONVERT" if use_force_convert else "NORMAL"
                msg['Subject'] = f"[verval-pupuk2] REKAP KLASTER BERHASIL {mode}"
                
                email_body = f"""
                <html>
                    <body>
                        <h2 style="color: green;">✅ REKAP KLASTER BERHASIL {mode}</h2>
                        <div style="background-color: #f0f8f0; padding: 15px; border-radius: 5px;">
                            <h3>📊 STATISTIK:</h3>
                            <p>• Total data: {len(combined_df):,}</p>
                            <p>• MENUNGGU_KEC: {status_counts.get('MENUNGGU_KEC', 0):,}</p>
                            <p>• DISETUJUI_KEC: {status_counts.get('DISETUJUI_KEC', 0):,}</p>
                            <p>• DISETUJUI_PUSAT: {status_counts.get('DISETUJUI_PUSAT', 0):,}</p>
                            <p>• Sheet Kecamatan: {kecamatan_sheet_count}</p>
                            <p>• Sheet Kios: {kios_sheet_count}</p>
                            <h3>🔗 LINK:</h3>
                            <p>• Kecamatan: <a href="{KECAMATAN_SHEET_URL}">{KECAMATAN_SHEET_URL}</a></p>
                            <p>• Kios: <a href="{KIOS_SHEET_URL}">{KIOS_SHEET_URL}</a></p>
                        </div>
                    </body>
                </html>
                """
                
                msg.attach(MIMEText(email_body, 'html'))
                
                with smtplib.SMTP("smtp.gmail.com", 587) as server:
                    server.starttls()
                    server.login(sender_email, sender_password)
                    server.send_message(msg)
                
                print(f"✅ Email notifikasi terkirim")
                
        except Exception as e:
            print(f"⚠️  Gagal kirim email: {str(e)}")
        
        print(f"\n🎉 PROSES SELESAI DENGAN SUKSES!")

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print(traceback.format_exc())

# ============================
# JALANKAN FUNGSI UTAMA
# ============================
if __name__ == "__main__":
    # Tanya user apakah mau pakai force convert
    print("=" * 80)
    print("🔧 PILIH MODE:")
    print("1. Mode Normal (gunakan klasifikasi biasa)")
    print("2. Mode Force Convert (ubah semua 'menunggu' ke 'disetujui')")
    print("=" * 80)
    
    try:
        choice = input("Pilih mode (1 atau 2, default 2): ").strip()
        use_force_convert = choice != "1"
        
        if use_force_convert:
            print("\n⚡ MENGGUNAKAN MODE FORCE CONVERT")
            print("   Semua status 'menunggu' akan diubah menjadi 'disetujui'")
        else:
            print("\n📋 MENGGUNAKAN MODE NORMAL")
        
        process_verval_pupuk_with_force_fix(use_force_convert=use_force_convert)
        
    except KeyboardInterrupt:
        print("\n\n❌ Proses dibatalkan oleh user")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
