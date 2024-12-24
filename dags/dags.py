from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from unidecode import unidecode

from vnwork import scrape_vietnamworks,save_data_to_csv


provinces_regions = {
    "Vung Trung Du Mien Nui Bac Bo": [
        "Ha Giang", "Cao Bang", "Lang Son", "Bac Giang", "Phu Tho", "Thai Nguyen", "Bac Kan",
        "Tuyen Quang", "Lao Cai", "Yen Bai", "Lai Chau", "Son La", "Dien Bien", "Hoa Binh"
    ],
    "Vung Dong Bang Song Hong": [
        "Ha Noi", "Hai Phong", "Hai Duong", "Hung Yen", "Vinh Phuc", "Bac Ninh", "Thai Binh",
        "Nam Dinh", "Ha Nam", "Ninh Binh", "Quang Ninh"
    ],
    "Vung Bac Trung Bo va Duyen Hai Mien Trung": [
        "Thanh Hoa", "Nghe An", "Ha Tinh", "Quang Binh", "Quang Tri", "Thua Thien Hue", "Da Nang",
        "Quang Nam", "Quang Ngai", "Binh Dinh", "Phu Yen", "Khanh Hoa", "Ninh Thuan", "Binh Thuan"
    ],
    "Vung Tay Nguyen": [
        "Kon Tum", "Gia Lai", "Dak Lak", "Dak Nong", "Lam Dong"
    ],
    "Vung Dong Nam Bo": [
        "Ho Chi Minh", "Dong Nai", "Ba Ria - Vung Tau", "Binh Duong", "Binh Phuoc", "Tay Ninh"
    ],
    "Vung Dong Bang Song Cuu Long": [
        "Can Tho", "Long An", "Tien Giang", "Ben Tre", "Tra Vinh", "Vinh Long", "An Giang", "Vinh Phuc",
        "Dong Thap", "Bac Lieu", "Kien Giang", "Soc Trang", "Hau Giang", "Ca Mau", "Bac Lieu"
    ]
}

regions_mien = {
    "Mien Bac": [
        "Ha Giang", "Cao Bang", "Lang Son", "Bac Giang", "Phu Tho", "Thai Nguyen", "Bac Kan",
        "Tuyen Quang", "Lao Cai", "Yen Bai", "Lai Chau", "Son La", "Dien Bien", "Hoa Binh", 
        "Ha Noi", "Hai Phong", "Hai Duong", "Hung Yen", "Vinh Phuc", "Bac Ninh", "Thai Binh",
        "Nam Dinh", "Ha Nam", "Ninh Binh", "Quang Ninh"
    ],
    "Mien Trung": [
        "Thanh Hoa", "Nghe An", "Ha Tinh", "Quang Binh", "Quang Tri", "Thua Thien Hue", "Da Nang",
        "Quang Nam", "Quang Ngai", "Binh Dinh", "Phu Yen", "Khanh Hoa", "Ninh Thuan", "Binh Thuan",
        "Kon Tum", "Gia Lai", "Dak Lak", "Dak Nong", "Lam Dong"
    ],
    "Mien Nam": [
        "Ho Chi Minh", "Dong Nai", "Ba Ria - Vung Tau", "Binh Duong", "Binh Phuoc", "Tay Ninh",
        "Can Tho", "Long An", "Tien Giang", "Ben Tre", "Tra Vinh", "Vinh Long", "Vinh Phuc",
        "Bac Lieu", "Kien Giang", "Soc Trang", "Hau Giang", "Ca Mau", "Dong Thap", "An Giang"
    ]
}

city_list = {
    "An Giang", "Ba Ria - Vung Tau", "Bac Giang", "Bac Kan", "Bac Lieu", "Bac Ninh",
    "Ben Tre", "Binh Duong", "Binh Dinh", "Binh Phuoc", "Binh Thuan", "Ca Mau",
    "Cao Bang", "Can Tho", "Da Nang", "Dak Lak", "Dak Nong", "Dien Bien", "Dong Nai",
    "Dong Thap", "Gia Lai", "Ha Giang", "Ha Nam", "Ha Noi", "Ha Tinh", "Hai Duong",
    "Hai Phong", "Hau Giang", "Hoa Binh", "Hung Yen", "Khanh Hoa", "Kien Giang",
    "Kon Tum", "Lai Chau", "Lam Dong", "Lang Son", "Lao Cai", "Long An", "Nam Dinh",
    "Nghe An", "Ninh Binh", "Ninh Thuan", "Phu Tho", "Phu Yen", "Quang Binh",
    "Quang Nam", "Quang Ngai", "Quang Ninh", "Quang Tri", "Soc Trang", "Son La",
    "Tay Ninh", "Thai Binh", "Thai Nguyen", "Thanh Hoa", "Thua Thien Hue", "Tien Giang",
    "Ho Chi Minh", "Tra Vinh", "Tuyen Quang", "Vinh Long", "Vinh Phuc", "Yen Bai"
}

mapping_jg = {
    'Điện/Điện Tử/Điện Lạnh' : 'Kỹ Thuật',
    'Truyền Thông/PR/Quảng Cáo' :'Tiếp Thị, Quảng Cáo/Truyền Thông',
    'Bán Sỉ/Bán Lẻ/Cửa Hàng':'Bán Hàng/Kinh Doanh',
    'Nhà Hàng/Khách Sạn' : 'Nhà Hàng - Khách Sạn/Du Lịch',
    'An Toàn Lao Động': 'Khác',
    'Giáo Dục/Đào Tạo': 'Giáo Dục',
    'Sản Xuất/Lắp Ráp/Chế Biến':'Sản Xuất',
    'Dệt May/Da Giày':'Dệt May/Da Giày',
    'Hóa Sinh': 'Khoa Học & Kỹ Thuật',
    'IT Phần Mềm': 'Công Nghệ Thông Tin/Viễn Thông',
    'Kinh Doanh/Bán Hàng':'Bán Hàng/Kinh Doanh',
    'Đấu Thầu/Dự Án': 'Bất Động Sản',
    'Cơ Khí/Ô Tô/Tự Động Hóa' : 'Khoa Học & Kỹ Thuật',
    'Biên Phiên Dịch': 'Biên Phiên Dịch',
    'In Ấn/Chế Bản' : 'Nghệ Thuật, Truyền Thông/In Ấn/Xuất Bản',
    'Nông/Lâm/Ngư Nghiệp': 'Nông/Lâm/Ngư Nghiệp',
    'Chăm Sóc Khách Hàng' : 'Dịch Vụ Khách Hàng',
    'Vận Hành/Bảo Trì/Bảo Dưỡng' : 'Kỹ Thuật',
    'IT Phần Cứng': 'Công Nghệ Thông Tin/Viễn Thông',
    'Nhân Sự': 'Nhân Sự/Tuyển Dụng',
    'Xuất Nhập Khẩu':'Hậu Cần/Xuất Nhập Khẩu/Kho Bãi',
    'Spa/Làm Đẹp':'Dịch Vụ Khách Hàng',
    'Viễn Thông': 'Công Nghệ Thông Tin/Viễn Thông',
    'Kế Toán/Kiểm Toán': 'Kế Toán/Kiểm Toán',
    'Luật/Pháp Chế':'Pháp Lý',
    'Truyền Hình/Báo Chí' : 'Truyền Hình/Báo Chí',
    'Khoa Học/Kỹ Thuật' : 'Khoa Học & Kỹ Thuật',
    'Thiết Kế': 'Thiết Kế',
    'Hành Chính/Văn Phòng': 'Hành Chính Văn Phòng',
    'Bảo Hiểm': 'Bảo Hiểm',
    'Y Tế':'Y Tế/Chăm Sóc Sức Khỏe',
    'Tài Chính/Ngân Hàng':'Ngân Hàng & Dịch Vụ Tài Chính',
    'Kiến Trúc/Nội Thất': 'Kiến Trúc/Xây Dựng',
    'Kho Vận': 'Hậu Cần/Xuất Nhập Khẩu/Kho Bãi',
    'Thu Mua': 'Kinh Doanh',
    'Dược Phẩm/Mỹ Phẩm': 'Dược',
    'Marketing':'Tiếp Thị, Quảng Cáo/Truyền Thông',
    'Lao Động Phổ Thông':'Khác',
    'Bất Động Sản':'Bất Động Sản',
    'Mỏ/Địa Chất': 'Khác',
    'Du Lịch': 'Nhà Hàng - Khách Sạn/Du Lịch',
    'Môi Trường': 'Khác',
    'Xây Dựng': 'Kiến Trúc/Xây Dựng',
    'Quản Lý': 'Ceo & General Management',
    'Sáng Tạo/Nghệ Thuật': 'Nghệ Thuật, Truyền Thông/In Ấn/Xuất Bản'
}

mapping_vw = {
    'Bán Lẻ/Tiêu Dùng' : 'Bán Hàng/Kinh Doanh',
    'Bảo Hiểm' : 'Bảo Hiểm',
    'Bất Động Sản' : 'Bất Động Sản',
    'Ceo & General Management' : 'Ceo & General Management',
    'Chính Phủ/Phi Lợi Nhuận' : 'Chính Phủ/Phi Lợi Nhuận',
    'Công Nghệ Thông Tin/Viễn Thông' :'Công Nghệ Thông Tin/Viễn Thông',
    'Dệt May/Da Giày' :'Dệt May/Da Giày' ,
    'Dịch Vụ Ăn Uống': 'Dịch Vụ Ăn Uống',
    'Dịch Vụ Khách Hàng' :'Dịch Vụ Khách Hàng',
    'Dược' :'Dược',
    'Giáo Dục':'Giáo Dục',
    'Hành Chính Văn Phòng':'Hành Chính Văn Phòng',
    'Hậu Cần/Xuất Nhập Khẩu/Kho Bãi':'Hậu Cần/Xuất Nhập Khẩu/Kho Bãi',
    'Kế Toán/Kiểm Toán':'Kế Toán/Kiểm Toán',
    'Khoa Học & Kỹ Thuật':'Khoa Học & Kỹ Thuật',
    'Kiến Trúc/Xây Dựng':'Kiến Trúc/Xây Dựng',
    'Kinh Doanh':'Kinh Doanh',
    'Kỹ Thuật':'Kỹ Thuật',
    'Ngân Hàng & Dịch Vụ Tài Chính':'Ngân Hàng & Dịch Vụ Tài Chính',
    'Nghệ Thuật, Truyền Thông/In Ấn/Xuất Bản':'Nghệ Thuật, Truyền Thông/In Ấn/Xuất Bản',
    'Nhà Hàng - Khách Sạn/Du Lịch':'Nhà Hàng - Khách Sạn/Du Lịch',
    'Nhân Sự/Tuyển Dụng':'Nhân Sự/Tuyển Dụng',
    'Nông/Lâm/Ngư Nghiệp':'Nông/Lâm/Ngư Nghiệp',
    'Pháp Lý':'Pháp Lý',
    'Sản Xuất':'Sản Xuất',
    'Thiết Kế':'Thiết Kế',
    'Tiếp Thị, Quảng Cáo/Truyền Thông':'Tiếp Thị, Quảng Cáo/Truyền Thông',
    'Vận Tải': 'Vận Tải',
    'Y Tế/Chăm Sóc Sức Khỏe':'Y Tế/Chăm Sóc Sức Khỏe'
}

mapping_job_level_vnwork = {
    'Giám Đốc Và Cấp Cao Hơn' : 'Giám Đốc',
    'Mới Tốt Nghiệp':'Mới Tốt Nghiệp',
    'Nhân Viên' : 'Nhân Viên',
    'Thực Tập Sinh/Sinh Viên' : 'Thực Tập Sinh',
    'Trưởng Phòng' : 'Trưởng Nhóm/Trưởng Phòng'
}

mapping_job_level_jg = {
    'Giám Đốc Và Cấp Cao Hơn' : 'Giám Đốc',
    'Mới Tốt Nghiệp/Thực Tập Sinh' : 'Mới Tốt Nghiệp',
    'Nhân Viên' : 'Nhân Viên',
    'Nhân Viên/Chuyên Viên' : 'Nhân Viên',
    'Thực Tập Sinh' : 'Thực Tập Sinh',
    'Trường Nhóm/Trưởng Phòng' : 'Trưởng Nhóm/Trưởng Phòng'
}

salary_range_to_id = {
    '0': 1,
    '< 300': 2,
    '300 - 500': 3,
    '500 - 1000': 4,
    '1000 - 2000': 5,
    '2000 - 5000': 6,
    '> 5000': 7
}

exp_range_to_id = {
    '0': 1,
    '0-1': 2,
    '1-2': 3,
    '2-5': 4,
    '5-10': 5,
    '>10': 6
}

city_id = {
    "An Giang": 1, "Ba Ria - Vung Tau": 2, "Bac Giang": 3, "Bac Kan": 4, "Bac Lieu": 5,
    "Bac Ninh": 6, "Ben Tre": 7, "Binh Duong": 8, "Binh Dinh": 9, "Binh Phuoc": 10,
    "Binh Thuan": 11, "Ca Mau": 12, "Cao Bang": 13, "Can Tho": 14, "Da Nang": 15,
    "Dak Lak": 16, "Dak Nong": 17, "Dien Bien": 18, "Dong Nai": 19, "Dong Thap": 20,
    "Gia Lai": 21, "Ha Giang": 22, "Ha Nam": 23, "Ha Noi": 24, "Ha Tinh": 25,
    "Hai Duong": 26, "Hai Phong": 27, "Hau Giang": 28, "Hoa Binh": 29, "Hung Yen": 30,
    "Khanh Hoa": 31, "Kien Giang": 32, "Kon Tum": 33, "Lai Chau": 34, "Lam Dong": 35,
    "Lang Son": 36, "Lao Cai": 37, "Long An": 38, "Nam Dinh": 39, "Nghe An": 40,
    "Ninh Binh": 41, "Ninh Thuan": 42, "Phu Tho": 43, "Phu Yen": 44, "Quang Binh": 45,
    "Quang Nam": 46, "Quang Ngai": 47, "Quang Ninh": 48, "Quang Tri": 49, "Soc Trang": 50,
    "Son La": 51, "Tay Ninh": 52, "Thai Binh": 53, "Thai Nguyen": 54, "Thanh Hoa": 55,
    "Thua Thien Hue": 56, "Tien Giang": 57, "Ho Chi Minh": 58, "Tra Vinh": 59,
    "Tuyen Quang": 60, "Vinh Long": 61, "Vinh Phuc": 62, "Yen Bai": 63, "Khac": 0
}

group_job_id = {
    'Bán Hàng/Kinh Doanh' : 1,
    'Bảo Hiểm' :2,
    'Bất Động Sản':3,
    'Ceo & General Management':4,
    'Chính Phủ/Phi Lợi Nhuận':5,
    'Công Nghệ Thông Tin/Viễn Thông':6,
    'Dệt May/Da Giày':7,
    'Dịch Vụ Ăn Uống':8,
    'Dịch Vụ Khách Hàng':9,
    'Dược':10,
    'Giáo Dục':11,
    'Hành Chính Văn Phòng':12,
    'Hậu Cần/Xuất Nhập Khẩu/Kho Bãi':13,
    'Kế Toán/Kiểm Toán':14,
    'Khoa Học & Kỹ Thuật':15,
    'Kiến Trúc/Xây Dựng':16,
    'Kinh Doanh':17,
    'Kỹ Thuật':18,
    'Ngân Hàng & Dịch Vụ Tài Chính':19,
    'Nghệ Thuật, Truyền Thông/In Ấn/Xuất Bản':20,
    'Nhà Hàng - Khách Sạn/Du Lịch':21,
    'Nhân Sự/Tuyển Dụng':22,
    'Nông/Lâm/Ngư Nghiệp':23,
    'Pháp Lý':24,
    'Sản Xuất':25,
    'Thiết Kế':26,
    'Tiếp Thị, Quảng Cáo/Truyền Thông':27,
    'Vận Tải':28,
    'Y Tế/Chăm Sóc Sức Khỏe':29,
    'Khác':0,
    'Biên Phiên Dịch':30,
    'Truyền Hình/Báo Chí':31
}

job_level_id = {
    'Giám Đốc':1,
    'Mới Tốt Nghiệp' :2,
    'Nhân Viên':3,
    'Thực Tập Sinh' : 4,
    'Tổng Giám Đốc' : 5,
    'Trưởng Nhóm/Trưởng Phòng':6,
    'Phó Giám Đốc' : 7,
    'Quản Lý':8,
    'Khác' : 0
}

web_id  = {
    'Jobsgo' : 1,
    'VietNamWork' : 2
}


def scrape_and_save():
    jobs_list = scrape_vietnamworks()
    save_data_to_csv(jobs_list)


def load_data_to_jobsgo(**kwargs):
    # Đọc dữ liệu từ CSV
    df = pd.read_csv('/opt/airflow/data/jobsgo.csv')

    current_time = datetime.now().strftime('%Y%m%d')
    df['id'] = df.index.map(lambda idx: f"jg{current_time}_{idx}")
    
    # Kết nối đến PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_jobsgo')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Chuẩn bị câu lệnh SQL INSERT
    for index, row in df.iterrows():
        insert_sql = """
            INSERT INTO Jobsgo (id, url, title, company_name, company_url, time_update, time_expire, salary, exp,
                                job_level, group_job, job_type, benefit, job_des, job_req, city, address, web)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_sql, (row['id'], row['url'],row['title'], row['company_name'], row['company_url'], row['time_update'],
                                    row['time_expire'], row['salary'], row['exp'], row['job_level'], row['group_job'], row['job_type'],
                                    row['benefit'], row['job_des'], row['job_req'], row['city'], row['address'], row['web']))
    
    # Commit và đóng kết nối
    conn.commit()
    cursor.close()

def load_data_to_vnwork(**kwargs):
    # Đọc dữ liệu từ CSV
    df = pd.read_csv('/opt/airflow/data/vnwork.csv')

    current_time = datetime.now().strftime('%Y%m%d')
    df['id'] = df.index.map(lambda idx: f"vn{current_time}_{idx}")
    
    # Kết nối đến PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_vnwork')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Chuẩn bị câu lệnh SQL INSERT
    for index, row in df.iterrows():
        insert_sql = """
            INSERT INTO Vnwork (id, url, title, company_name, company_url, time_update, time_expire, salary, exp,
                                job_level, group_job, job_type, benefit, job_des, job_req, city, address, web)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_sql, (row['id'], row['url'],row['title'], row['company_name'], row['company_url'], row['time_update'],
                                    row['time_expire'], row['salary'], row['exp'], row['job_level'], row['group_job'], row['job_type'],
                                    row['benefit'], row['job_des'], row['job_req'], row['city'], row['address'], row['web']))
    
    # Commit và đóng kết nối
    conn.commit()
    cursor.close()


def categorize_salary(salary):
    if salary == 0:
        return '0'
    elif salary < 300:
        return '< 300'
    elif 300 <= salary < 500:
        return '300 - 500'
    elif 500 <= salary < 1000:
        return '500 - 1000'
    elif 1000 <= salary < 2000:
        return '1000 - 2000'
    elif 2000 <= salary < 5000:
        return '2000 - 5000'
    else:
        return '> 5000'

def categorize_experience(exp):
    if exp == 0:
        return '0'
    elif 0 < exp <= 1:
        return '0-1'
    elif 1 < exp <= 2:
        return '1-2'    
    elif 2 < exp <= 5:
        return '2-5'
    elif 5 < exp <= 10:
        return '5-10'
    else:
        return '>10'

def clean_and_transform_jg(df):
    # Loại bỏ các dòng có giá trị NaN ở các cột quan trọng
    df = df.dropna(subset=['url', 'salary', 'group_job', 'city','time_update'])

    current_time = datetime.now().strftime('%Y%m%d')
    df['id'] = df.index.map(lambda idx: f"jg{current_time}_{idx}")

    # Áp dụng unidecode để loại bỏ dấu cho cột 'city' và 'company_name'
    df['city'] = df['city'].apply(unidecode)
    df['company_name'] = df['company_name'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    # Chuyển đổi giá trị cột 'job_level' và 'group_job' về chữ cái đầu tiên viết hoa
    df['job_level'] = df['job_level'].str.title()
    df['group_job'] = df['group_job'].str.title()

    # Áp dụng unidecode cho 'company_name' và 'city'
    df['company_name'] = df['company_name'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)
    df['city'] = df['city'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    # Kiểm tra nếu giá trị 'city' có trong danh sách các thành phố, nếu không thì thay thế bằng 'Khac'
    df['city'] = df['city'].apply(lambda x: x if x in city_list else 'Khac')

    # Thay thế giá trị trong cột 'group_job' và 'job_level' theo bản đồ đã định nghĩa
    df['group_job'] = df['group_job'].map(mapping_jg).fillna('Khác')
    df['job_level'] = df['job_level'].map(mapping_job_level_jg).fillna('Khác')

    return df


def clean_and_transform_vw(df):
    # Loại bỏ các dòng có giá trị NaN ở các cột quan trọng
    df = df.dropna(subset=['url', 'salary', 'group_job', 'city', 'time_update'])

    current_time = datetime.now().strftime('%Y%m%d')
    df['id'] = df.index.map(lambda idx: f"vn{current_time}_{idx}")

    # Áp dụng unidecode để loại bỏ dấu cho cột 'city' và 'company_name'
    df['city'] = df['city'].apply(unidecode)
    df['company_name'] = df['company_name'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    # Chuyển đổi giá trị cột 'job_level' và 'group_job' về chữ cái đầu tiên viết hoa
    df['job_level'] = df['job_level'].str.title()
    df['group_job'] = df['group_job'].str.title()

    # Áp dụng unidecode cho 'company_name' và 'city'
    df['company_name'] = df['company_name'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)
    df['city'] = df['city'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    # Kiểm tra nếu giá trị 'city' có trong danh sách các thành phố, nếu không thì thay thế bằng 'Khac'
    df['city'] = df['city'].apply(lambda x: x if x in city_list else 'Khac')

    # Thay thế giá trị trong cột 'group_job' và 'job_level' theo bản đồ đã định nghĩa
    df['group_job'] = df['group_job'].map(mapping_vw).fillna('Khác')
    df['job_level'] = df['job_level'].map(mapping_job_level_vnwork).fillna('Khác')

    # Trả về DataFrame đã được xử lý
    return df


def process_clean_data():
    input_jobsgo_file = '/opt/airflow/data/jobsgo.csv'
    input_vnwork_file = '/opt/airflow/data/vnwork.csv'
    output_file = '/opt/airflow/data/data_final.csv'

    df_jobsgo = pd.read_csv(input_jobsgo_file)
    df_vnwork = pd.read_csv(input_vnwork_file)

    df_jobsgo_cleaned = clean_and_transform_jg(df_jobsgo)
    df_vnwork_cleaned = clean_and_transform_vw(df_vnwork)

    df = pd.concat([df_jobsgo_cleaned, df_vnwork_cleaned], ignore_index=True)

    # Tạo cột 'eco_region' và 'region' dựa trên tên thành phố
    df['eco_region'] = df['city'].map(lambda city: next((region for region, provinces in provinces_regions.items() if city in provinces), 'Khac'))
    df['region'] = df['city'].map(lambda city: next((mien for mien, provinces in regions_mien.items() if city in provinces), 'Khac'))

    # Phân loại mức lương và kinh nghiệm
    df['salary_range'] = df['salary'].apply(categorize_salary)
    df['exp_range'] = df['exp'].apply(categorize_experience)

    # Thêm các cột ID của mức lương và kinh nghiệm
    df['salary_range_id'] = df['salary_range'].map(salary_range_to_id)
    df['exp_range_id'] = df['exp_range'].map(exp_range_to_id)
    df['city_id'] = df['city'].map(city_id)
    df['group_job_id'] = df['group_job'].map(group_job_id)
    df['job_level_id'] = df['job_level'].map(job_level_id).fillna(0)
    df['web_id'] = df['web'].map(web_id)

    df['time_update'] = pd.to_datetime(df['time_update'], errors='coerce')

    # Tạo cột năm, quý, tháng
    df['time_year'] = df['time_update'].dt.year  # Năm
    df['time_quarter'] = df['time_update'].dt.quarter  # Quý
    df['time_month'] = df['time_update'].dt.month  # Tháng

    df.to_csv(output_file, index=False, encoding='utf-8-sig')

def create_dim_fact():
    df = pd.read_csv("/opt/airflow/data/data_final.csv")

    dim_address = df[['city_id','city', 'eco_region', 'region']].drop_duplicates().reset_index(drop=True)

    dim_date = df[['time_update','time_month','time_quarter','time_year']].drop_duplicates().reset_index(drop=True)

    dim_web = df[['web_id','web']].drop_duplicates().reset_index(drop=True)

    dim_joblevel = df[['job_level_id','job_level']].drop_duplicates().reset_index(drop=True)

    dim_groupjob = df[['group_job_id','group_job']].drop_duplicates().reset_index(drop=True)

    dim_exprange = df[['exp_range_id','exp_range']].drop_duplicates().reset_index(drop=True)

    dim_salaryrange = df[['salary_range_id', 'salary_range']].drop_duplicates().reset_index(drop=True)

    fact_job = df[['id', 'title', 'url', 'job_type', 'group_job_id', 'job_level_id', 'time_update', 
                  'time_expire', 'company_name', 'company_url', 'salary', 'salary_range_id', 'city_id',
                  'address', 'benefit', 'job_des', 'job_req', 'exp_range_id', 'exp', 'web_id']]

    dim_address.to_csv('/opt/airflow/data/dim_address.csv', index=False, encoding='utf-8-sig')
    dim_date.to_csv('/opt/airflow/data/dim_date.csv', index=False, encoding='utf-8-sig')
    dim_web.to_csv('/opt/airflow/data/dim_web.csv', index=False, encoding='utf-8-sig')
    dim_joblevel.to_csv('/opt/airflow/data/dim_joblevel.csv', index=False, encoding='utf-8-sig')
    dim_groupjob.to_csv('/opt/airflow/data/dim_groupjob.csv', index=False, encoding='utf-8-sig')
    dim_exprange.to_csv('/opt/airflow/data/dim_exprange.csv', index=False, encoding='utf-8-sig')
    dim_salaryrange.to_csv('/opt/airflow/data/dim_salaryrange.csv', index=False, encoding='utf-8-sig')
    fact_job.to_csv('/opt/airflow/data/fact_job.csv', index=False, encoding='utf-8-sig')

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_olap')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #INSERT Dim_Address
    for index, row in dim_address.iterrows():
        insert_sql = """
            INSERT INTO Dim_Address (CityID, CityName, EcoRegion, Region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (CityID)
            DO UPDATE 
            SET CityName = EXCLUDED.CityName,
                EcoRegion = EXCLUDED.EcoRegion,
                Region = EXCLUDED.Region;
        """

        cursor.execute(insert_sql, (row['city_id'], row['city'],row['eco_region'], row['region']))
    
    #INSERT Dim_Date
    for index, row in dim_date.iterrows():
        insert_sql = """
            INSERT INTO Dim_Date (DateID, DateMonth, DateQuarter, DateYear)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (DateID)
            DO UPDATE 
            SET DateMonth = EXCLUDED.DateMonth,
                DateQuarter = EXCLUDED.DateQuarter,
                DateYear = EXCLUDED.DateYear;
        """

        cursor.execute(insert_sql, (row['time_update'], row['time_month'], row['time_quarter'], row['time_year']))

    #INSERT Dim_Web
    for index, row in dim_web.iterrows():
        insert_sql = """
            INSERT INTO Dim_Web (WebID, Web)
            VALUES (%s, %s)
            ON CONFLICT (WebID)
            DO UPDATE 
            SET Web = EXCLUDED.Web;
        """

        cursor.execute(insert_sql, (row['web_id'], row['web']))

    #INSERT Dim_JobLevel
    for index, row in dim_joblevel.iterrows():
        insert_sql = """
            INSERT INTO Dim_JobLevel (JobLevelID, JobLevelName)
            VALUES (%s, %s)
            ON CONFLICT (JobLevelID)
            DO UPDATE 
            SET JobLevelName = EXCLUDED.JobLevelName;
        """

        cursor.execute(insert_sql, (row['job_level_id'], row['job_level']))

    #INSERT Dim_GroupJob
    for index, row in dim_groupjob.iterrows():
        insert_sql = """
            INSERT INTO Dim_GroupJob (GroupJobID, GroupJobName)
            VALUES (%s, %s)
            ON CONFLICT (GroupJobID)
            DO UPDATE 
            SET GroupJobName = EXCLUDED.GroupJobName;
        """

        cursor.execute(insert_sql, (row['group_job_id'], row['group_job']))

    #INSERT Dim_ExpRange
    for index, row in dim_exprange.iterrows():
        insert_sql = """
            INSERT INTO Dim_ExpRange (ExpRangeID, ExpRange)
            VALUES (%s, %s)
            ON CONFLICT (ExpRangeID)
            DO UPDATE 
            SET ExpRange = EXCLUDED.ExpRange;
        """

        cursor.execute(insert_sql, (row['exp_range_id'], row['exp_range']))

    #INSERT Dim_SalaryRange
    for index, row in dim_salaryrange.iterrows():
        insert_sql = """
            INSERT INTO Dim_SalaryRange (SalaryRangeID, SalaryRange)
            VALUES (%s, %s)
            ON CONFLICT (SalaryRangeID)
            DO UPDATE 
            SET SalaryRange = EXCLUDED.SalaryRange;
        """

        cursor.execute(insert_sql, (row['salary_range_id'], row['salary_range']))

    #INSERT Fact_Job
    for index, row in fact_job.iterrows():
        insert_sql = """
            INSERT INTO Fact_Job (JobID, JobTitle, JobURL, JobType, GroupJobID, JobLevelID, DateID,
                                    TimeExpire, CompanyName, CompanyURL, Salary, SalaryRangeID,
                                    CityID, Address, Benefit, JobDes, JobReq, ExpRangeID, Exp, WebID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(insert_sql, (row['id'], row['title'], row['url'], row['job_type'], row['group_job_id'],
                                    row['job_level_id'], row['time_update'], row['time_expire'], row['company_name'],
                                    row['company_url'], row['salary'], row['salary_range_id'], row['city_id'], row['address'],
                                    row['benefit'], row['job_des'], row['job_req'], row['exp_range_id'], row['exp'], row['web_id']))

    
    # Commit và đóng kết nối
    conn.commit()
    cursor.close()


with DAG(
    dag_id = "da",
    start_date= datetime(2024,12,1),
    schedule_interval='0 0 * * 1'
) as dag:

    run_spider_jobsgo = BashOperator(
        task_id = 'run_jobsgospider',
        bash_command = "cd /opt/airflow/jobsgoscraper && scrapy crawl jobsgospider -O /opt/airflow/data/jobsgo.csv"
    )

    run_vnwork = PythonOperator(
        task_id='run_vnwork',
        python_callable=scrape_and_save,
    )
    
    create_table_postgres_jobsgo = PostgresOperator(
        task_id = 'create_table_jobsgo',
        postgres_conn_id='postgres_conn_jobsgo',
        sql="""
            CREATE TABLE IF NOT EXISTS Jobsgo (
                id varchar(500) primary key,
                url varchar(500) ,
                title varchar(500),
                company_name varchar(500),
                company_url varchar(500),
                time_update timestamp,
                time_expire timestamp,
                salary decimal(10,2),
                exp decimal(10,2),
                job_level varchar(500),
                group_job varchar(500),
                job_type varchar(500),
                benefit text,
                job_des text,
                job_req text,
                city varchar(500),
                address text,
                web varchar(500)
            );"""
    )

    create_table_postgres_vnwork = PostgresOperator(
        task_id = 'create_table_vnwork',
        postgres_conn_id='postgres_conn_vnwork',
        sql="""
            CREATE TABLE IF NOT EXISTS Vnwork (
                id varchar(500) primary key,
                url varchar(500) ,
                title varchar(500),
                company_name varchar(500),
                company_url varchar(500),
                time_update timestamp,
                time_expire timestamp,
                salary decimal(10,2),
                exp decimal(10,2),
                job_level varchar(500),
                group_job varchar(500),
                job_type varchar(500),
                benefit text,
                job_des text,
                job_req text,
                city varchar(500),
                address text,
                web varchar(500)
            );"""
    )

    load_data_jobsgo = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_jobsgo,
        provide_context=True
    )

    load_data_vnwork = PythonOperator(
        task_id='load_data_to_vnwork',
        python_callable=load_data_to_vnwork,
        provide_context=True
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=process_clean_data
    )

    create_olap = PostgresOperator(
        task_id = 'create_table_olap',
        postgres_conn_id = 'postgres_conn_olap',
        sql = """
            CREATE TABLE IF NOT EXISTS Dim_Date (
                DateID DATE PRIMARY KEY NOT NULL,
                DateMonth INT,
                DateQuarter INT,
                DateYear INT
            );

            -- Dim_Web
            CREATE TABLE IF NOT EXISTS Dim_Web (
                WebID INT PRIMARY KEY NOT NULL,
                Web VARCHAR(500)
            );

            -- Dim_JobLevel
            CREATE TABLE IF NOT EXISTS Dim_JobLevel(
                JobLevelID INT PRIMARY KEY NOT NULL,
                JobLevelName VARCHAR(500)
            );

            -- Dim_GroupJob
            CREATE TABLE IF NOT EXISTS Dim_GroupJob(
                GroupJobID INT PRIMARY KEY NOT NULL,
                GroupJobName VARCHAR(500)
            );

            -- Dim_Address
            CREATE TABLE IF NOT EXISTS Dim_Address(
                CityID INT PRIMARY KEY,
                CityName VARCHAR(500),
                EcoRegion VARCHAR(500),
                Region VARCHAR(500)
            );

            -- Dim_ExpRange
            CREATE TABLE IF NOT EXISTS Dim_ExpRange(
                ExpRangeID INT PRIMARY KEY,
                ExpRange VARCHAR(500)
            );

            -- Dim_SalaryRange
            CREATE TABLE IF NOT EXISTS Dim_SalaryRange(
                SalaryRangeID INT PRIMARY KEY,
                SalaryRange VARCHAR(500)
            );

            -- Fact_Job
            CREATE TABLE IF NOT EXISTS Fact_Job(
                JobID VARCHAR(500) PRIMARY KEY,
                JobTitle TEXT,
                JobURL VARCHAR(3000),
                JobType VARCHAR(500),
                GroupJobID INT,
                JobLevelID INT,
                DateID DATE,
                TimeExpire DATE,
                CompanyName VARCHAR(500),
                CompanyURL TEXT,
                Salary DECIMAL(10,2),
                SalaryRangeID INT,
                CityID INT,
                Address TEXT,
                Benefit TEXT,
                JobDes TEXT,
                JobReq TEXT,
                ExpRangeID INT,
                Exp DECIMAL(10,2),
                WebID INT,

                FOREIGN KEY (GroupJobID) REFERENCES Dim_GroupJob(GroupJobID),
                FOREIGN KEY (JobLevelID) REFERENCES Dim_JobLevel(JobLevelID),
                FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
                FOREIGN KEY (CityID) REFERENCES Dim_Address(CityID),
                FOREIGN KEY (SalaryRangeID) REFERENCES Dim_SalaryRange(SalaryRangeID),
                FOREIGN KEY (ExpRangeID) REFERENCES Dim_ExpRange(ExpRangeID),
                FOREIGN KEY (WebID) REFERENCES Dim_Web(WebID)
            );"""
    )

    load_data_olap = PythonOperator(
        task_id='load_data_to_olap',
        python_callable=create_dim_fact,
        provide_context=True
    )

run_vnwork >> run_spider_jobsgo >> [create_table_postgres_jobsgo, create_table_postgres_vnwork] >> clean_data_task >> create_olap >> load_data_olap
create_table_postgres_jobsgo >> load_data_jobsgo
create_table_postgres_vnwork >> load_data_vnwork