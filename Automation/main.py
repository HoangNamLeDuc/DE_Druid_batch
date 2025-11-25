# wkdir = data/date

from datetime import datetime, timedelta
import schedule
import time
from fetch_sources import fetch_then_save_raw_symbols
from merge_files import merge_concat_all, join_with_ICB100
import metadata as md

# Chạy feed dữ liệu đầu:
def init_data():
    start_date = '01/01/2020'
    end_date = '11/21/2025'
    md.change_default_dict(start_date) #Cập nhật lại metadata
    wkdir = f"data/init"  
    fetch_then_save_raw_symbols(md.vn100_symbols, start_date, end_date, page_size=1000, wkdir=wkdir, output_format='csv', sources=md.sources)
    output_file_path = merge_concat_all(md.vn100_symbols, wkdir, output_file="merged_stock_data.csv")
    join_with_ICB100(file_merged=output_file_path, wkdir=wkdir)

# Chạy mỗi đêm
# parallel đc không?
def job(): 
    today_date = datetime.today() #  - timedelta(days=1)
    print(f"start job, today is {today_date}")

    # Kiểm tra nếu hôm nay là thứ 7 (Saturday) hoặc chủ nhật (Sunday), nếu đúng thì bỏ qua
    if today_date.weekday() in [5, 6]:  # 5 là Saturday, 6 là Sunday
        print("Today is a weekend (Saturday or Sunday), skipping the job.")
        return
    
    # Chỉnh lại format ngày trước khi gọi hàm
    today_date_wkdir = today_date.strftime('%Y-%m-%d')
    today_date_param = today_date.strftime('%m/%d/%Y')
    today_date_default = today_date.strftime('%d/%m/%Y')
    md.change_default_dict(default_date = today_date_default) #Cập nhật lại metadata
    start_date = today_date_param
    end_date = today_date_param
    wkdir = f"data/{today_date_wkdir}"  

    fetch_then_save_raw_symbols(md.vn100_symbols, start_date, end_date, page_size=100, wkdir=wkdir, output_format='csv', sources=md.sources)
    output_file_path = merge_concat_all(md.vn100_symbols, wkdir, output_file="merged_stock_data.csv")
    join_with_ICB100(file_merged=output_file_path, wkdir=wkdir)

# test
job()

schedule.every().day.at("22:00").do(job)  # Chạy lúc 10 giờ tối mỗi ngày

while True:
    schedule.run_pending()  # Chạy các tác vụ đã lên lịch
    time.sleep(60) # Check kẻo lỡ lệch phát là ăn cám đấy
