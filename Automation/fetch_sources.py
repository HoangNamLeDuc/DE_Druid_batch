import requests
import pandas as pd
import os 
import metadata as md


#3 Lấy thông tin về giao dịch trong ngày của Công ty, xuất ra csv
#3.1 Hàm để lấy dữ liệu lịch sử giá từ API
def get_stock_data(symbol, start_date, end_date, page_index=1, page_size=20, from_source="PriceHistory"):
    """
    Lấy dữ liệu từ API tùy thuộc vào `from_source` và trả về dữ liệu tương ứng.

    :param symbol: Mã cổ phiếu
    :param start_date: Ngày bắt đầu
    :param end_date: Ngày kết thúc
    :param page_index: Trang dữ liệu
    :param page_size: Kích thước trang
    :param from_source: Chỉ định nguồn dữ liệu (PriceHistory, ThongKeDL, GDKhoiNgoai, GDTuDoanh)
    :return: Dữ liệu cổ phiếu từ API dưới dạng danh sách
    """
    
    src_url = md.source_columns[from_source]
    # if(from_source == "GDTuDoanh"): API lỗi lắm!
    #   start_date=''
    #   end_date=''
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/{from_source}.ashx?Symbol={symbol}&StartDate={start_date}&EndDate={end_date}&PageIndex={page_index}&PageSize={page_size}"
    
    # Gửi yêu cầu GET và nhận dữ liệu JSON
    response = requests.get(url)
    data = response.json()
    stock_data = []
    # Trích xuất dữ liệu từ JSON (theo cấu trúc bạn cung cấp)
    if(from_source == "GDTuDoanh"):
      # Đối với 'GDTuDoanh', dữ liệu có thể nằm sâu hơn trong 'Data' và 'ListDataTudoanh'
      if data['Data'] != None: 
        stock_data = data['Data']['Data']['ListDataTudoanh']
      # else:
      #   stock_data = [{
      #     'Symbol': symbol,
      #     'Date': '01/01/2020',
      #     'KLcpMua': 0,
      #     'KlcpBan': 0,
      #     'GtMua': 0,
      #     'GtBan': 0}]
    else:
      if data.get('Data') and data['Data'].get('Data'):
        stock_data = data['Data']['Data']
      # else: lại rỗng
    
    return stock_data

#3.2 ETL Task, Hàm để chuyển dữ liệu JSON thành DataFrame
def json_to_dataframe(symbol, stock_data, from_source="PriceHistory"):
    listOfField = md.source_columns[from_source]
    prefix = md.prefix_map[from_source]
    
    # Chuyển dữ liệu thành DataFrame
    df = pd.DataFrame(stock_data)
    # Kiểm tra nếu DataFrame là empty
    if df.empty:
        # Nếu dữ liệu trống, tạo một bảng dữ liệu mặc định
        print(f"Warning: No data available for {symbol}. Creating default data.")
        # Tạo DataFrame với dữ liệu mặc định
        default_data = md.default_data_dict[from_source]
        # Nếu là GDTuDoanh thì cẩn thận
        if(from_source == "GDTuDoanh"):
            default_data[0]['Symbol'] = symbol
        df = pd.DataFrame(default_data)

    df = df[listOfField]
    

    # Thống nhất field name Tiếng Việt
    # Nếu có col 'Date', chuyển nó thành 'Ngay'
    if 'Date' in df.columns:
        df = df.rename(columns={'Date': 'Ngay'})  
        
    # Duyệt qua tất cả cột và thêm tiền tố (prefix) vào tên cột
    df = df.rename(columns={col: prefix + col if col != 'Ngay' else col for col in df.columns})

    # Chuyển cột 'Ngay' sang định dạng ngày tháng
    df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y')
    
    return df

#3.3 ETL Task, Hàm để chuyển dữ liệu JSON thành DataFrame
def json_to_dataframe(symbol, stock_data, from_source="PriceHistory"):
    listOfField = md.source_columns[from_source]
    prefix = md.prefix_map[from_source]
    
    # Chuyển dữ liệu thành DataFrame
    df = pd.DataFrame(stock_data)
    # Kiểm tra nếu DataFrame là empty
    if df.empty:
        # Nếu dữ liệu trống, tạo một bảng dữ liệu mặc định
        print(f"Warning: No data available for {symbol}. Creating default data.")
        # Tạo DataFrame với dữ liệu mặc định
        default_data = md.default_data_dict[from_source]
        # Nếu là GDTuDoanh thì cẩn thận
        if(from_source == "GDTuDoanh"):
            default_data[0]['Symbol'] = symbol
        df = pd.DataFrame(default_data)

    df = df[listOfField]

    # Làm task Transform 1 chút trước khi lưu
    # Thống nhất field name Tiếng Việt
    # Nếu có col 'Date', chuyển nó thành 'Ngay'
    if 'Date' in df.columns:
        df = df.rename(columns={'Date': 'Ngay'})  
        
    # Duyệt qua tất cả cột và thêm tiền tố (prefix) vào tên cột
    df = df.rename(columns={col: prefix + col if col != 'Ngay' else col for col in df.columns})

    # Chuyển cột 'Ngay' sang định dạng ngày tháng
    df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y')
    
    return df


#3.4 Hàm để lấy và lưu dữ liệu từ API
def save_stock_data(symbol, start_date, end_date, page_size=100, wkdir='data', output_format='csv', from_source="PriceHistory"):
    all_data = []
    page_index = 1
    
    # Lặp qua các trang dữ liệu (nếu cần thiết)
    while True:
        print(f"Fetching data for page {page_index}...")
        stock_data = get_stock_data(symbol, start_date, end_date, page_index, page_size, from_source)
        if not stock_data:            
            break
        
        all_data.extend(stock_data)
        page_index += 1

    # Chuyển dữ liệu thành DataFrame
    df = json_to_dataframe(symbol, all_data, from_source)
    
    # Lưu dữ liệu vào file theo định dạng yêu cầu
    # Tạo thư mục để lưu dữ liệu (nếu chưa tồn tại)
    dir_path = f"{wkdir}/{symbol}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    file_name = f"{symbol}_stock_data_{from_source}"
    file_path = os.path.join(dir_path, file_name)
    if output_format == 'csv':
        df.to_csv(f'{file_path}.csv', index=False)
        print(f"Data saved to {file_path}.csv")
    elif output_format == 'json':
        df.to_json(f'{file_path}.json', orient='records', lines=True)
        print(f"Data saved to {file_path}.json")

# save_stock_data('HPG', '01/01/2022', '11/23/2025', page_size=1000, wkdir='data', output_format='csv', from_source=md.sources[3])

#3.5 Chạy để fetch và lưu cả 4 sources cho 1 symbol
def fetch_then_save_raw(symbol, start_date, end_date, page_size=100, wkdir='data', output_format='csv', sources=md.sources):
    for source in sources:
        save_stock_data(symbol, start_date, end_date, page_size, wkdir=wkdir, output_format='csv', from_source=source)

# loop qua từng symbols
def fetch_then_save_raw_symbols(symbols, start_date, end_date, page_size=100, wkdir='data', output_format='csv', sources=md.sources):
    for symbol in symbols:
        print(f"---------------Crawl data {symbol}---------------------")
        fetch_then_save_raw(symbol, start_date, end_date, page_size=page_size, wkdir=wkdir, output_format=output_format)

# Ví dụ sử dụng hàm:
# fetch_then_save_raw_symbols(md.vn100_symbols, start_date, end_date, page_size=1000, wkdir='data', output_format='csv', sources=md.sources)
