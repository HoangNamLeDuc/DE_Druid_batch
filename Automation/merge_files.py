import os
import pandas as pd
import fetch_sources as fetchSs 
import metadata as md

#1 ETL Tạo file merge các columns lại từ các sources 
#1.1 ETL Task, Join các bảng trong cùng 1 công ty (symbol) thành 1 file
def load_merge(symbol, wkdir, output_format='csv'):
    dir_path = f"{wkdir}/{symbol}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    dfs = {}
    # Load tất cả các file vào dictionary
    for source in md.sources:
        file_name = f"{symbol}_stock_data_{source}.csv"
        file_path = os.path.join(dir_path, file_name)
        dfs[source] = pd.read_csv(file_path)
        print(f"Loaded {file_name} into DataFrame")

    # Merge các bảng lại với nhau theo cột 'Ngay'
    df_all = dfs["PriceHistory"]
    for source in md.sources[1:]:  # Bắt đầu từ phần tử thứ 2, vì df_price đã là phần đầu tiên
        df_all = df_all.merge(dfs[source], on='Ngay', how='outer')

    df_all.insert(1, 'symbol', symbol)
    # Kiểm tra kết quả
    # print(df_all.head())

    # Lưu kết quả vào một file mới
    output_file = f"{symbol}_stock_data_merged.csv"
    file_path = os.path.join(dir_path, output_file)
    df_all.to_csv(file_path, index=False)
    print(f"Data merged and saved to {file_path}")

# Test
# load_join("ACB", output_format='csv')
#1.2 Traverse qua tất cả symbols và thực hiện hàm trên
def load_merge_symbols(symbols, wkdir='data', output_format='csv'):
    for symbol in symbols:
        print(f"---------------Merged data {symbol}---------------------")
        load_merge(symbol, wkdir, output_format=output_format)

#2. ETL Task, Concat các bảng trong cùng 1 wkdir lại thành 1 file và sort
def concat_all(symbols_list, wkdir="data", output_file="merged_stock_data.csv", to_sort=True):
    all_data = []  # Danh sách chứa các DataFrame

    # Duyệt qua danh sách các symbol (các folder)
    for symbol in symbols_list:
        dir_path = f"{wkdir}/{symbol}"

        # Kiểm tra folder có tồn tại và có file merged không
        file_path = os.path.join(dir_path, f"{symbol}_stock_data_merged.csv")
        if os.path.exists(file_path):
            # Đọc file merged vào DataFrame
            df = pd.read_csv(file_path)
            df['symbol'] = symbol  # Thêm cột symbol vào DataFrame
            all_data.append(df)
            print(f"Loaded {file_path}")
        else:
            print(f"File {file_path} not found")

    # Nếu tất cả file đều không tồn tại
    if not all_data:
        print("No files found to merge!")
        return

    # Concatenate tất cả DataFrame trong all_data
    merged_data = pd.concat(all_data, ignore_index=True)

    # Sort theo ngày
    if(to_sort):
        # Chuyển cột 'Ngay' sang datetime và sort theo cột 'Ngay'
        merged_data['Ngay'] = pd.to_datetime(merged_data['Ngay'])
        merged_data = merged_data.sort_values(by='Ngay')

    # Lưu kết quả vào file CSV
    output_file_path = os.path.join(wkdir, output_file)
    merged_data.to_csv(output_file_path, index=False)
    print(f"Data merged and saved to {output_file_path}")
    return output_file_path

# Ví dụ sử dụng hàm:
# concat_all(md.vn100_symbols, wkdir="data", output_file="vn100_stock_data_merged.csv")

#3. Merge và Concat mọi thứ lại
def merge_concat_all(symbols_list, wkdir, output_file="merged_stock_data.csv", to_sort=True):
    load_merge_symbols(symbols_list, wkdir, output_format='csv')
    return concat_all(symbols_list, wkdir, output_file, to_sort)
    
#4. Join với ICB 100
def join_with_ICB100(file_merged, file_icb='VN100_with_ICB.csv', wkdir='data'):
    # Load dữ liệu từ các file CSV
    df_icb = pd.read_csv(file_icb)
    df_merged = pd.read_csv(file_merged)

    # Merge theo cột 'symbol'
    df_joined = pd.merge(df_merged, df_icb, on="symbol", how="inner")

    # Kiểm tra kết quả
    # print(df_joined.head())

    # Lưu kết quả vào một file mới
    output_file = "vn100_stock_data_merged_with_ICB.csv"
    output_file_path = os.path.join(wkdir, output_file)
    df_joined.to_csv(output_file_path, index=False)

    print(f"Data joined and saved to {output_file_path}")
