# wkdir = data/date

from datetime import datetime
import schedule
import time
from fetch_sources import fetch_then_save_raw_symbols
from merge_files import merge_concat_all
import metadata as md

# Chạy feed dữ liệu đầu:
def init_data():
    start_date = '01/01/2020'
    end_date = '11/21/2025'
    md.change_default_dict(start_date) #Cập nhật lại metadata
    wkdir = f"data/init"  
    fetch_then_save_raw_symbols(md.vn100_symbols, start_date, end_date, page_size=1000, wkdir=wkdir, output_format='csv', sources=md.sources)
    merge_concat_all(md.vn100_symbols, wkdir, output_file="merged_stock_data.csv")


# Chạy mỗi đêm
# parallel đc không?
def job(): 
    today_date = datetime.today()
    print(f"start job, today is {today_date}")
    # Kiểm tra nếu hôm nay là thứ 7 (Saturday) hoặc chủ nhật (Sunday), nếu đúng thì bỏ qua
    if today_date.weekday() in [5, 6]:  # 5 là Saturday, 6 là Sunday
        print("Today is a weekend (Saturday or Sunday), skipping the job.")
        return
    today_date = today_date.strftime('%Y-%m-%d')
    md.change_default_dict(default_date = datetime.today().strftime('%d/%m/%Y')) #Cập nhật lại metadata
    start_date = today_date
    end_date = today_date
    wkdir = f"data/{today_date}"  
    fetch_then_save_raw_symbols(md.vn100_symbols, start_date, end_date, page_size=1000, wkdir=wkdir, output_format='csv', sources=md.sources)
    merge_concat_all(md.vn100_symbols, wkdir, output_file="merged_stock_data.csv")

# test
job()

schedule.every().day.at("22:00").do(job)  # Chạy lúc 10 giờ tối mỗi ngày

while True:
    schedule.run_pending()  # Chạy các tác vụ đã lên lịch
    time.sleep(60) # Check kẻo lỡ lệch phát là ăn cám đấy
