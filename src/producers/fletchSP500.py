import json
import pandas as pd
import requests
import io
import os
CWD_path = os.getcwd() 

def save_sp500_symbols():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    # GIẢ DANH TRÌNH DUYỆT (Quan trọng)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # 1. Tải nội dung HTML về trước với headers
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Báo lỗi nếu tải thất bại
        
        # 2. Đưa nội dung HTML vào pandas
        # Sử dụng io.StringIO để tránh cảnh báo trong các phiên bản pandas mới
        tables = pd.read_html(io.StringIO(response.text))
        df = tables[0] # Bảng đầu tiên thường là danh sách công ty
        
        # 3. Lấy cột Symbol
        symbols = df['Symbol'].tolist()
        
        # 4. Format lại cho yfinance (thay dấu chấm bằng gạch ngang, VD: BRK.B -> BRK-B)
        symbols = [s.replace('.', '-') for s in symbols]
        
        # 5. Lưu vào file txt
        filename = os.path.join(CWD_path, 'IDE','SP500_symbol.txt')
        with open(filename, 'w') as f:
            for symbol in symbols:
                f.write(f"{symbol}\n")
                
        print(f"✅ Thành công! Đã lưu {len(symbols)} mã vào file '{filename}'")
        
    except Exception as e:
        print(f"❌ Vẫn lỗi: {e}")

if __name__ == "__main__":
    save_sp500_symbols()