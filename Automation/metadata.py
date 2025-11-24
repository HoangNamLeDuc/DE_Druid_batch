import csv
from datetime import datetime

#1 Lấy dữ liệu cho vn100_symbols để iterate fetch
vn100_symbols = []
with open('vn100_symbols.csv', mode='r', newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile)
    # next(csv_reader, None)
    for row in csv_reader:
        vn100_symbols.append(row[1])

#2 MetaData
#2.1 Thông tin về src 
'''
sources là 4 nhóm dữ liệu đi fetch
columns_list là những col trả về từ file json get được
source_columns là zip sources với columns_list
prefix_map để đổi tên cho những col trong df/ csv file sau này
default_data_dict get mà không được gì thì mặc định lấy nó
'''
sources = ["PriceHistory", "ThongKeDL", "GDKhoiNgoai", "GDTuDoanh"]
columns_list = [
    ['Ngay', 'GiaDieuChinh', 'GiaDongCua', 'ThayDoi', 'KhoiLuongKhopLenh', 'GiaTriKhopLenh', 
     'KLThoaThuan', 'GtThoaThuan', 'GiaMoCua', 'GiaCaoNhat', 'GiaThapNhat'],  # PriceHistory
     
    ['Date', 'ThayDoi', 'SoLenhMua', 'KLDatMua', 'KLTB1LenhMua', 'SoLenhDatBan', 'KLDatBan', 
     'KLTB1LenhBan', 'ChenhLechKL'],  # ThongKeDL
     
    ['Ngay', 'KLGDRong', 'GTDGRong', 'ThayDoi', 'KLMua', 'GtMua', 'KLBan', 'GtBan', 
     'RoomConLai', 'DangSoHuu'],  # GDKhoiNgoai
     
    ['Symbol', 'Date', 'KLcpMua', 'KlcpBan', 'GtMua', 'GtBan']  # GDTuDoanh
]
source_columns = dict(map(lambda x, y: (x, y), sources, columns_list))

prefix_map = {
    'PriceHistory': 'LS_',
    'ThongKeDL': 'DL_',
    'GDKhoiNgoai': 'KN_',
    'GDTuDoanh': 'TD_'
}
#2.2 Dữ liệu mặc định cho từng nguồn
default_date = datetime.today().strftime('%d/%m/%Y')
# default_date = '01/01/2022'
default_data_dict = {
    'PriceHistory': [{
        'Ngay': default_date,
        'GiaDieuChinh': 0,
        'GiaDongCua': 0,
        'ThayDoi': 0,
        'KhoiLuongKhopLenh': 0,
        'GiaTriKhopLenh': 0,
        'KLThoaThuan': 0,
        'GtThoaThuan': 0,
        'GiaMoCua': 0,
        'GiaCaoNhat': 0,
        'GiaThapNhat': 0
    }],
    
    'ThongKeDL': [{
        'Date': default_date,
        'ThayDoi': 0,
        'SoLenhMua': 0,
        'KLDatMua': 0,
        'KLTB1LenhMua': 0,
        'SoLenhDatBan': 0,
        'KLDatBan': 0,
        'KLTB1LenhBan': 0,
        'ChenhLechKL': 0
    }],
    
    'GDKhoiNgoai': [{
        'Ngay': default_date,
        'KLGDRong': 0,
        'GTDGRong': 0,
        'ThayDoi': 0,
        'KLMua': 0,
        'GtMua': 0,
        'KLBan': 0,
        'GtBan': 0,
        'RoomConLai': 0,
        'DangSoHuu': 0
    }],
    
    'GDTuDoanh': [{
        'Symbol': 'symbol',  # Sử dụng 'symbol' để thay thế sau khi dữ liệu thực tế được lấy
        'Date': default_date,
        'KLcpMua': 0,
        'KlcpBan': 0,
        'GtMua': 0,
        'GtBan': 0
    }]
}
