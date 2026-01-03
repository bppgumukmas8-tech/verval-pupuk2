# ============================================
# TAMBAHKAN TANGGAL INPUT KE SHEET
# ============================================
if latest_tanggal_input:
    print(f"\n📅 Menambahkan informasi tanggal input ke sheet kecamatan_all...")
    
    # Cari sheet 'kecamatan_all' dan tambahkan informasi tanggal
    try:
        spreadsheet = gc.open_by_url(OUTPUT_SHEET_URL)
        worksheet = spreadsheet.worksheet("kecamatan_all")
        
        # Update kolom E
        worksheet.update('E1', 'Update per tanggal input')
        time.sleep(WRITE_DELAY)
        
        tanggal_str = latest_tanggal_input.strftime('%d %b %Y')
        worksheet.update('E2', tanggal_str)
        time.sleep(WRITE_DELAY)
        
        jam_str = latest_tanggal_input.strftime('%H:%M:%S')
        worksheet.update('E3', jam_str)
        
        print(f"   ✅ Informasi tanggal ditambahkan di kolom E:")
        print(f"      E1: 'Update per tanggal input'")
        print(f"      E2: {tanggal_str}")
        print(f"      E3: {jam_str}")
        
        # Format kolom E
        try:
            date_format = {
                "backgroundColor": {
                    "red": 0.95,
                    "green": 0.95,
                    "blue": 0.85
                },
                "textFormat": {
                    "bold": True
                }
            }
            worksheet.format('E1:E3', date_format)
        except:
            pass
            
    except Exception as e:
        print(f"   ⚠️  Gagal menambahkan informasi tanggal: {e}")
else:
    print(f"   ⚠️  Tidak ada tanggal input yang valid untuk ditambahkan")
