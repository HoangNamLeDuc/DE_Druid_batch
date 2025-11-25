# wkdir = data/date

from datetime import datetime, timedelta
# import schedule
import time
from fetch_sources import fetch_then_save_raw_symbols
from merge_files import merge_concat_all, join_with_ICB100
import metadata as md
import os
import requests
import json

url = "http://localhost:8888/druid/indexer/v1/task"

def create_task_json(wkdir: str = '/data', file_name: str = 'vn100_stock_data_merged_with_ICB.csv'):
    # Tạo đường dẫn đến thư mục chứa tệp JSON
    base_dir = os.path.join("/home/dongphan/Desktop/andong/DE_Druid_batch/Automation/", wkdir)

    # Kiểm tra nếu thư mục chưa tồn tại, tạo thư mục mới
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # Đường dẫn đầy đủ đến tệp JSON
    task_file = os.path.join(base_dir, 'task.json')

    # Cấu trúc JSON, với baseDir thay đổi theo ngày
    task_data = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": base_dir,  # Thay thế theo biến date
                    "filter": file_name
                },
                "inputFormat": {
                    "type": "csv",
                    "findColumnsFromHeader": True
                },
                "appendToExisting": True
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": 500,
                    "maxTotalRows": 200000
                }
            },
            "dataSchema": {
                "dataSource": "inline_data",
                "timestampSpec": {
                    "column": "Ngay",
                    "format": "auto"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "symbol",
                        {"type": "long", "name": "LS_GiaDieuChinh"},
                        {"type": "long", "name": "LS_GiaDongCua"},
                        {"type": "long", "name": "LS_ThayDoi"},
                        {"type": "long", "name": "LS_KhoiLuongKhopLenh"},
                        {"type": "long", "name": "LS_GiaTriKhopLenh"},
                        {"type": "long", "name": "LS_KLThoaThuan"},
                        {"type": "long", "name": "LS_GtThoaThuan"},
                        {"type": "long", "name": "LS_GiaMoCua"},
                        {"type": "long", "name": "LS_GiaCaoNhat"},
                        {"type": "long", "name": "LS_GiaThapNhat"},
                        "DL_ThayDoi",
                        {"type": "long", "name": "DL_SoLenhMua"},
                        {"type": "long", "name": "DL_KLDatMua"},
                        {"type": "long", "name": "DL_KLTB1LenhMua"},
                        {"type": "long", "name": "DL_SoLenhDatBan"},
                        {"type": "long", "name": "DL_KLDatBan"},
                        {"type": "long", "name": "DL_KLTB1LenhBan"},
                        "DL_ChenhLechKL",
                        {"type": "long", "name": "KN_KLGDRong"},
                        {"type": "long", "name": "KN_GTDGRong"},
                        {"type": "long", "name": "KN_ThayDoi"},
                        {"type": "long", "name": "KN_KLMua"},
                        {"type": "long", "name": "KN_GtMua"},
                        {"type": "long", "name": "KN_KLBan"},
                        {"type": "long", "name": "KN_GtBan"},
                        {"type": "long", "name": "KN_RoomConLai"},
                        {"type": "long", "name": "KN_DangSoHuu"},
                        "TD_Symbol",
                        {"type": "long", "name": "TD_KLcpMua"},
                        {"type": "long", "name": "TD_KlcpBan"},
                        {"type": "long", "name": "TD_GtMua"},
                        {"type": "long", "name": "TD_GtBan"},
                        "group",
                        "organ_name",
                        "icb_name3",
                        "icb_name2",
                        "icb_name4",
                        "com_type_code",
                        {"type": "long", "name": "icb_code1"},
                        {"type": "long", "name": "icb_code2"},
                        {"type": "long", "name": "icb_code3"},
                        {"type": "long", "name": "icb_code4"}
                    ]
                },
                "granularitySpec": {
                    "queryGranularity": "none",
                    "rollup": False,
                    "segmentGranularity": "month"
                }
            }
        }
    }

    # Ghi dữ liệu JSON vào tệp task.json
    with open(task_file, 'w') as f:
        json.dump(task_data, f, indent=2)

    print(f"task.json created at {task_file}")
    return task_file

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
    today_date = datetime.today() - timedelta(days=1)
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

    task_file = create_task_json(wkdir)
    # Đọc nội dung tệp task.json
    with open(task_file, 'r') as file:
        task_data = json.load(file)
    response = requests.post(url, json=task_data)
    if response.status_code == 200:
        print("Task submitted successfully!")
    else:
        print(f"Failed to submit task. Status code: {response.status_code}")
        print(f"Response: {response.text}")


# test
job()

# schedule.every().day.at("22:00").do(job)  # Chạy lúc 10 giờ tối mỗi ngày

# while True:
#     schedule.run_pending()  # Chạy các tác vụ đã lên lịch
#     time.sleep(60) # Check kẻo lỡ lệch phát là ăn cám đấy
