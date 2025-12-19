import os
import json
import pandas as pd
import gspread
import re
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ============================
# KONFIGURASI
# ============================
# Sumber data: Sheet Sisa dari script sebelumnya
SOURCE_SPREADSHEET_ID = "1GRo7BP1a2MdEjZEnUVVDQBqp7KuLOViRlxMOkL0ERJE"
SOURCE_SHEET_NAME = "Sisa"

# Target: Spreadsheet untuk output WA
TARGET_SPREADSHEET_ID = "1u6owIr34B5c2aA6FcgncMGUv7AqQHHI--8Z9e-aI_mo"
TARGET_SHEET_NAME = "Sisa versi Wa"

# ============================
# KONFIGURASI EMAIL (SECRETS)
# ============================
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS")  # pisah dengan koma

EMAIL_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": SENDER_EMAIL,
    "sender_password": SENDER_EMAIL_PASSWORD,
    "recipient_emails": [email.strip() for email in RECIPIENT_EMAILS.split(",")] if RECIPIENT_EMAILS else []
}

# ============================
# FUNGSI BERSIHKAN NIK
# ============================
def clean_nik(nik_value):
    """Membersihkan NIK dari karakter non-angka"""
    if pd.isna(nik_value) or nik_value is None:
        return None

    nik_str = str(nik_value)
    cleaned_nik = re.sub(r'\D', '', nik_str)

    if len(cleaned_nik) != 16:
        print(f"⚠️  NIK tidak standar: {nik_value} -> {cleaned_nik} (panjang: {len(cleaned_nik)})")

    return cleaned_nik if cleaned_nik else None

# ============================
# FUNGSI FORMAT PUPUK
# ============================
def format_pupuk_value(value):
    """Format nilai pupuk menjadi integer jika .00, tetap desimal jika perlu"""
    try:
        if pd.isna(value) or value is None:
            return "0"
        
        float_val = float(value)
        if float_val.is_integer():
            return str(int(float_val))
        else:
            formatted = f"{float_val:.2f}"
            return formatted.rstrip('0').rstrip('.')
    except:
        return "0"

# ============================
# FUNGSI BUAT TEKS WA PER BARIS - TAMPILKAN SEMUA PUPUK
# ============================
def create_wa_text(row, index):
    """Membuat teks WA untuk satu baris data pupuk - TAMPILKAN SEMUA JENIS PUPUK"""
    try:
        # Format nama kios
        nama_kios = str(row.get('NAMA_KIOS', '')).strip()
        if not nama_kios:
            nama_kios = "Kios Tanpa Nama"
        
        # Daftar semua jenis pupuk dengan nilai default 0
        pupuk_types = {
            'SISA_UREA': 'Urea',
            'SISA_NPK': 'NPK', 
            'SISA_SP36': 'SP36',
            'SISA_ZA': 'ZA',
            'SISA_NPK_FORMULA': 'NPK Formula',
            'SISA_ORGANIK': 'Organik',
            'SISA_ORGANIK_CAIR': 'Organik Cair'
        }
        
        # Ambil semua nilai pupuk, format semua
        pupuk_data = []
        for col_key, pupuk_name in pupuk_types.items():
            value = row.get(col_key, 0)
            formatted_value = format_pupuk_value(value)
            pupuk_data.append(f"{pupuk_name} {formatted_value} kg")
        
        # Gabungkan semua pupuk
        pupuk_text = ", ".join(pupuk_data)
        
        return f"{index}) {nama_kios} - {pupuk_text}"
    
    except Exception as e:
        print(f"❌ Error creating WA text for row {index}: {e}")
        return f"{index}) Error processing data"

# ============================
# FUNGSI BUAT TEKS WA UTAMA
# ============================
def create_complete_wa_text(wa_items):
    """Membuat teks WA lengkap dengan header"""
    if not wa_items:
        return "Sisa kuota anda :\nTidak ada data sisa kuota"
    
    header = "Sisa kuota anda :"
    items_text = "\n".join(wa_items)
    
    return f"{header}\n{items_text}"

# ============================
# FUNGSI KIRIM EMAIL
# ============================
def send_email_notification(subject, message, is_success=True):
    """Mengirim notifikasi email (menggunakan secrets)"""
    try:
        # Validasi konfigurasi email
        if (
            not EMAIL_CONFIG.get("sender_email")
            or not EMAIL_CONFIG.get("sender_password")
            or not EMAIL_CONFIG.get("recipient_emails")
        ):
            print("❌ Konfigurasi email belum lengkap (cek secrets)")
            return False

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

        with smtplib.SMTP(
            EMAIL_CONFIG["smtp_server"],
            EMAIL_CONFIG["smtp_port"]
        ) as server:
            server.starttls()
            server.login(
                EMAIL_CONFIG["sender_email"],
                EMAIL_CONFIG["sender_password"]
            )
            server.send_message(msg)

        print(f"📧 Email terkirim ke {EMAIL_CONFIG['recipient_emails'][0]}")
        return True

    except Exception as e:
        print(f"❌ Gagal mengirim email: {str(e)}")
        return False

# ============================
# FUNGSI PROSES DATA DENGAN ERROR HANDLING
# ============================
def process_sisa_kuota_wa():
    """Proses utama: Baca data dari sheet Sisa, rekap per NIK untuk WA"""
    print("=" * 60)
    print("🚀 MEMULAI PROSES SISA KUOTA WA")
    print("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # ============================================
        # BAGIAN 1: LOAD CREDENTIALS
        # ============================================
        print("\n🔑 Loading credentials...")
        
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
        print("✅ Credentials berhasil di-load")
        
        # ============================================
        # BAGIAN 2: BACA DATA DARI SHEET SISA
        # ============================================
        print(f"\n📥 Baca data dari sheet '{SOURCE_SHEET_NAME}'...")
        
        source_spreadsheet = gc.open_by_key(SOURCE_SPREADSHEET_ID)
        
        try:
            source_worksheet = source_spreadsheet.worksheet(SOURCE_SHEET_NAME)
            print(f"✅ Sheet '{SOURCE_SHEET_NAME}' ditemukan")
        except Exception as e:
            print(f"❌ Sheet '{SOURCE_SHEET_NAME}' tidak ditemukan: {e}")
            raise
        
        print("📊 Membaca data dari Google Sheets...")
        try:
            data = source_worksheet.get_all_records()
            if not data:
                print("⚠️  Tidak ada data di sheet Sisa")
                return False
            
            df = pd.DataFrame(data)
            print(f"✅ Data loaded: {len(df)} baris")
            
            print(f"📋 Kolom yang ditemukan ({len(df.columns)} kolom):")
            for i, col in enumerate(df.columns, 1):
                print(f"   {i}. {col}")
                
        except Exception as e:
            print(f"❌ Gagal membaca data dari sheet: {e}")
            raise
        
        # Pastikan kolom yang diperlukan ada
        required_columns = ['NIK', 'NAMA_PETANI', 'NAMA_KIOS']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Kolom tidak ditemukan: {missing_columns}")
            for req_col in missing_columns:
                for actual_col in df.columns:
                    if req_col.lower() in actual_col.lower():
                        print(f"   • Mapping '{actual_col}' -> '{req_col}'")
                        df = df.rename(columns={actual_col: req_col})
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Kolom yang diperlukan tidak ditemukan: {missing_columns}")
        
        # Pastikan kolom pupuk ada, jika tidak buat dengan nilai 0
        pupuk_columns = ['SISA_UREA', 'SISA_NPK', 'SISA_SP36', 'SISA_ZA', 
                        'SISA_NPK_FORMULA', 'SISA_ORGANIK', 'SISA_ORGANIK_CAIR']
        
        for col in pupuk_columns:
            if col not in df.columns:
                print(f"⚠️  Kolom {col} tidak ditemukan, membuat dengan nilai 0")
                df[col] = 0
        
        # Bersihkan NIK
        print("\n🧹 Membersihkan NIK...")
        df['NIK_ORIGINAL'] = df['NIK'].copy()
        df['NIK'] = df['NIK'].apply(clean_nik)
        
        # Hapus baris dengan NIK kosong
        initial_count = len(df)
        df = df[df['NIK'].notna()].copy()
        print(f"   • Setelah cleaning: {len(df)} baris (dihapus {initial_count - len(df)} baris)")
        
        # ============================================
        # BAGIAN 3: REKAP DATA PER NIK
        # ============================================
        print("\n📊 Membuat rekap data per NIK...")
        
        output_rows = []
        
        df_sorted = df.sort_values(['NIK', 'NAMA_KIOS']).reset_index(drop=True)
        nik_groups = df_sorted.groupby('NIK')
        total_nik = len(nik_groups)
        
        print(f"   • Total NIK unik: {total_nik}")
        
        for nik_idx, (nik, group) in enumerate(nik_groups, start=1):
            try:
                nama_petani = ""
                if len(group) > 0:
                    nama_petani = group['NAMA_PETANI'].iloc[0]
                    if pd.isna(nama_petani):
                        nama_petani = ""
                
                nama_petani = str(nama_petani).strip()
                
                wa_items = []
                for idx, (_, row) in enumerate(group.iterrows(), start=1):
                    wa_text = create_wa_text(row, idx)
                    wa_items.append(wa_text)
                
                complete_wa_text = create_complete_wa_text(wa_items)
                
                output_rows.append({
                    'NIK': nik,
                    'NAMA_PETANI': nama_petani,
                    'DATA': complete_wa_text
                })
                
                if nik_idx % 10 == 0 or nik_idx == total_nik:
                    print(f"   • Diproses: {nik_idx}/{total_nik} NIK")
                    
            except Exception as e:
                print(f"❌ Error processing NIK {nik}: {e}")
                output_rows.append({
                    'NIK': nik,
                    'NAMA_PETANI': 'ERROR',
                    'DATA': f'Error processing data for NIK {nik}'
                })
        
        output_df = pd.DataFrame(output_rows, columns=['NIK', 'NAMA_PETANI', 'DATA'])
        print(f"✅ Rekap selesai: {len(output_df)} NIK unik")
        
        # ============================================
# BAGIAN 4: TULIS KE SHEET TARGET (DENGAN BATCH UPDATE)
# ============================================
print(f"\n📤 Menulis hasil ke sheet '{TARGET_SHEET_NAME}'...")

target_spreadsheet = gc.open_by_key(TARGET_SPREADSHEET_ID)

try:
    target_worksheet = target_spreadsheet.worksheet(TARGET_SHEET_NAME)
    print(f"   • Sheet '{TARGET_SHEET_NAME}' sudah ada, menghapus isi...")
    # Gunakan batch_clear untuk efisiensi
    target_worksheet.batch_clear(["A:Z"])
except Exception as e:
    if "not found" in str(e).lower():
        print(f"   • Sheet '{TARGET_SHEET_NAME}' tidak ada, membuat baru...")
        target_worksheet = target_spreadsheet.add_worksheet(
            title=TARGET_SHEET_NAME,
            rows=max(len(output_df) + 100, 1000),
            cols=len(output_df.columns)
        )
    else:
        raise e

print("   • Menyiapkan data untuk batch update...")

# 1. Format Header (Opsional, tetapi membuat sheet rapi)
try:
    header_format = {
        "backgroundColor": {"red": 0.2, "green": 0.6, "blue": 0.8},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}
    }
    target_worksheet.format('A1:C1', header_format)
except Exception as e:
    print(f"   ⚠️  Gagal memformat header: {e}")

# 2. Menyiapkan data untuk ditulis dalam SATU BATCH
# Konversi DataFrame menjadi list of lists (format yang diterima oleh update)
data_to_write = [output_df.columns.values.tolist()] + output_df.values.tolist()

# 3. Tentukan range target (dari A1 hingga kolom terakhir dan baris terakhir)
end_column = chr(64 + len(output_df.columns))  # Misal, jika 3 kolom -> 'C'
end_row = len(data_to_write)
target_range = f'A1:{end_column}{end_row}'

print(f"   • Menulis {len(data_to_write)-1} baris data ke range {target_range}...")

# 4. Gunakan batch update: SEMUA DATA DITULIS DALAM SATU PANGGILAN API
try:
    target_worksheet.update(
        values=data_to_write,
        range_name=target_range
    )
    print(f"✅ Data berhasil ditulis dalam satu batch update: {len(output_df)} baris")
except Exception as e:
    print(f"❌ Gagal dalam batch update utama: {e}")
    # Fallback: coba tulis secara bertahap dengan jeda eksponensial
    print("   • Mencoba fallback dengan exponential backoff...")
    import time, random
    
    for i in range(len(data_to_write)):
        # Tulis per baris untuk fallback
        row_range = f'A{i+1}:{end_column}{i+1}'
        row_data = [data_to_write[i]]
        
        max_retries = 5
        for retry in range(max_retries):
            try:
                target_worksheet.update(values=row_data, range_name=row_range)
                break
            except Exception as inner_e:
                if "429" in str(inner_e) and retry < max_retries - 1:
                    wait_time = (2 ** retry) + (random.random() * 1000) / 1000.0
                    print(f"     ⚠️  Rate limit pada baris {i+1}, retry {retry+1} dalam {wait_time:.2f} detik...")
                    time.sleep(wait_time)
                else:
                    raise inner_e
        
        # Jeda kecil antar baris untuk menghindari rate limit
        if i % 10 == 0 and i > 0:
            time.sleep(1)
    
    print(f"✅ Data berhasil ditulis (fallback method): {len(output_df)} baris")
        
        # ============================================
        # BAGIAN 5: BUAT SUMMARY DAN KIRIM EMAIL
        # ============================================
        print("\n📋 Membuat summary...")
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        total_rows = len(df)
        unique_nik = len(output_df)
        
        nik_counts = df_sorted.groupby('NIK').size()
        if len(nik_counts) > 0:
            avg_rows_per_nik = nik_counts.mean()
            max_rows_per_nik = nik_counts.max()
            min_rows_per_nik = nik_counts.min()
        else:
            avg_rows_per_nik = 0
            max_rows_per_nik = 0
            min_rows_per_nik = 0
        
        total_pupuk = {}
        for pupuk_col in pupuk_columns:
            if pupuk_col in df.columns:
                try:
                    df[pupuk_col] = pd.to_numeric(df[pupuk_col], errors='coerce')
                    total = df[pupuk_col].sum(skipna=True)
                    total_pupuk[pupuk_col] = total
                except:
                    total_pupuk[pupuk_col] = 0
        
        success_message = f"""
SISA KUOTA WA BERHASIL DIBUAT ✓

⏰ Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s
📅 Tanggal: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

📊 STATISTIK DATA:
- Total baris data: {total_rows}
- NIK unik: {unique_nik}
- Rata-rata data per NIK: {avg_rows_per_nik:.1f} baris

📊 TOTAL SISA PUPUK:"""
        
        for pupuk_col, total in total_pupuk.items():
            pupuk_name = pupuk_col.replace('SISA_', '')
            success_message += f"\n- {pupuk_name}: {total:,.2f} kg"
        
        success_message += f"""

📝 FORMAT OUTPUT WA (SEMUA JENIS PUPUK):
Sisa kuota anda :
1) [Nama Kios] - Urea [nilai] kg, NPK [nilai] kg, SP36 [nilai] kg, ZA [nilai] kg, NPK Formula [nilai] kg, Organik [nilai] kg, Organik Cair [nilai] kg
2) ... (dst)

📤 HASIL OUTPUT:
- Spreadsheet: https://docs.google.com/spreadsheets/d/{TARGET_SPREADSHEET_ID}
- Sheet: {TARGET_SHEET_NAME}

✅ Semua jenis pupuk ditampilkan (termasuk yang nilainya 0).
"""
        
        print(f"\n📊 SUMMARY:")
        print(f"   • Total data: {total_rows} baris")
        print(f"   • NIK unik: {unique_nik}")
        print(f"   • Waktu proses: {duration.seconds // 60}m {duration.seconds % 60}s")
        
        print(f"\n📋 SAMPLE OUTPUT (NIK pertama):")
        if len(output_df) > 0:
            sample_row = output_df.iloc[0]
            print(f"\n   NIK: {sample_row['NIK']}")
            print(f"   Nama: {sample_row['NAMA_PETANI']}")
            print(f"   Data WA (3 baris pertama):")
            
            data_lines = sample_row['DATA'].split('\n')
            for i, line in enumerate(data_lines[:4]):
                if i < 4:
                    print(f"   • {line}")
            
            # Tampilkan format pupuk
            print(f"\n   📝 Format: Semua jenis pupuk ditampilkan (nilai 0 juga ditampilkan)")
        
        print(f"\n📧 Mengirim email notifikasi...")
        email_sent = send_email_notification("SISA KUOTA WA BERHASIL", success_message, is_success=True)
        
        if email_sent:
            print("✅ Email berhasil dikirim")
        
        print(f"\n✅ PROSES SELESAI! 🎉")
        print(f"📋 Data WA tersedia di: https://docs.google.com/spreadsheets/d/{TARGET_SPREADSHEET_ID}")
        print(f"   • Semua jenis pupuk ditampilkan (termasuk nilai 0)")
        
        return True
        
    except Exception as e:
        error_message = f"""
PROSES SISA KUOTA WA GAGAL ❌

📅 Waktu: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
⚠️ Error: {str(e)}

🔧 Traceback:
{traceback.format_exc()}
"""
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()
        
        try:
            send_email_notification("SISA KUOTA WA GAGAL", error_message, is_success=False)
        except:
            pass
        
        return False

# ============================
# JALANKAN SCRIPT
# ============================
if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    
    process_sisa_kuota_wa()
