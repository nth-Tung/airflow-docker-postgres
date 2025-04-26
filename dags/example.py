import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from tempfile import NamedTemporaryFile

default_args = {
    'owner':'nttung',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

def transform():
    import pandas as pd
    import numpy as np
    from unidecode import unidecode
    import re
    import json

    df = pd.read_csv(f"dags/Full_Quan_5.csv", encoding="utf-8")

    df.columns = ["price", "address", "house_type", "acreage", "width", "length", "bedrooms", "bathrooms", "floors",
                  "legal_status"]

    df.dropna(inplace=True)

    # Pháp lý
    df["legal_status"] = df["legal_status"].astype(str).apply(lambda x: unidecode(x).strip().lower())

    # Loại nhà
    df["house_type"] = df["house_type"].astype(str).apply(lambda x: unidecode(x).strip().lower())


    def convert_price(value):
        value = value.lower().replace(",", ".").strip()
        try:
            if "tỷ" in value:
                return float(value.replace("tỷ", "").strip())
            elif "triệu" in value:
                return float(value.replace("triệu", "").strip()) / 1000
        except ValueError:
            return None
        return None


    # Giá nhà
    df['price'] = df['price'].apply(convert_price)

    # Chiều ngang
    df['width'] = df['width'].str.extract(r'([\d.]+)').astype(float).round(2)

    # Chiều dài
    df['length'] = df['length'].str.extract(r'([\d.]+)').astype(float).round(2)

    # Diện tích
    df['acreage'] = (df['width'] * df['length']).round(2)

    # Phòng ngủ
    df['bedrooms'] = df['bedrooms'].replace('nhiều hơn 10 phòng', '11')
    df['bedrooms'] = df['bedrooms'].str.extract(r'(\d+)').astype(int)

    # Phòng tắm
    df['bathrooms'] = df['bathrooms'].replace('Nhiều hơn 6 phòng', '7')
    df['bathrooms'] = df['bathrooms'].str.extract(r'(\d+)').astype(int)

    # Số tầng
    df['floors'] = df['floors'].astype(int)

    # Tách Đường, Phường, Quận, Thành Phố
    df[['street', 'ward', 'district', 'city']] = df['address'].str.rsplit(',', n=3, expand=True)
    # Chuẩn hóa dữ liệu: loại bỏ dấu và chuyển thường
    for col in ['street', 'ward', 'district', 'city']:
        df[col] = df[col].astype(str).apply(lambda x: unidecode(x).strip().lower())

    df = df.drop(columns=['address'])


    # Hàm trích tên đường
    def extract_ten_duong(diachi):
        # Nếu có từ 'duong' thì lấy từ sau nó
        match = re.search(r'duong\s+([a-z0-9\s\-]+)', diachi)
        if match:
            return match.group(1).strip()
        # Nếu không có, lấy cụm chữ cái đầu tiên (bỏ số nhà)
        match = re.search(r'^[0-9\-\/]*\s*([a-z\s\-]+?),', diachi)
        if match:
            return match.group(1).strip()
        return None


    # Áp dụng hàm trích xuất
    df['street'] = df['street'].apply(extract_ten_duong).apply(lambda x: "duong " + x if isinstance(x, str) else x)

    df.replace(["", " ", "null", "None"], np.nan, inplace=True)
    df = df.dropna()

    ten_duong = [
      "duong tran tuong cong",
      "duong tan hang",
      "duong nguyen thi nho",
      "duong ham tu",
      "duong tran tuan khai",
      "duong tang bat ho",
      "duong nguyen thi",
      "duong hai thuong lan ong",
      "duong tran phu",
      "duong tan da",
      "duong nguyen kim",
      "duong ha ton quyen",
      "duong tran nhan ton",
      "duong ta uyen",
      "duong nguyen duy duong",
      "duong go cong",
      "duong tran hung dao",
      "duong su van hanh",
      "duong nguyen chi thanh",
      "duong gia phu",
      "duong tran hoa",
      "duong phuoc hung",
      "duong nguyen bieu",
      "duong do van suu",
      "duong tran dien",
      "duong phung hung",
      "duong nguyen an",
      "duong do ngoc thach",
      "duong tran chanh chieu",
      "duong phu dong thien vuong",
      "duong nguyen an khuong",
      "duong dang thai than",
      "duong tran van kieu",
      "duong phu huu",
      "duong ngo quyen",
      "duong dao tan",
      "duong tang bat ho",
      "duong phu giao",
      "duong ngo nhan tinh",
      "duong duong tu giang",
      "duong tan da",
      "duong phu dinh",
      "duong ngo gia tu",
      "duong chieu anh cac",
      "duong yet kieu",
      "duong pho co dieu",
      "duong nghia thuc",
      "duong chau van liem",
      "duong xom voi",
      "duong phan van tri",
      "duong mac thien tich",
      "duong cao dat",
      "duong vu chi hieu",
      "duong phan van khoe",
      "duong mac cuu",
      "duong bui huu nghia",
      "duong vo truong toan",
      "duong phan phu tien",
      "duong ly thuong kiet",
      "duong bai say",
      "duong van tuong",
      "duong phan huy chu",
      "duong luu xuan tin",
      "duong bach van",
      "duong van kiep",
      "duong pham huu chi",
      "duong luong nhu hoc",
      "duong ba trieu",
      "duong trinh hoai duc",
      "duong pham don",
      "duong le hong phong",
      "duong an duong vuong",
      "duong trieu quang phuc",
      "duong pham ban",
      "duong lao tu",
      "duong an diem",
      "duong tran xuan hoa",
      "duong nhieu tam",
      "duong ky hoa",
      "duong an binh",
      "duong trang tu",
      "duong nguyen van dung",
      "duong kim bien",
      "duong vo truong toan",
      "duong tong duy tan",
      "duong nguyen tri phuong",
      "duong hung long",
      "duong yet kieu",
      "duong tan thanh",
      "duong nguyen trai",
      "duong huynh man dat",
      "duong hong bang",
      "duong tan hung",
      "duong nguyen thoi trung",
      "duong hung vuong",
      "duong hoc lac"
    ]
    df = df[df['street'].isin(ten_duong)]

    # Tách giá trị hiếm biệt thự
    df = df[df['house_type'] != 'nha biet thu']


    def remove_outliers(df, columns):
        for col in columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 5 * IQR
            upper_bound = Q3 + 5 * IQR
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
        return df


    # Xóa outlies
    numeric_columns = ['price', 'width', 'length', 'floors']
    df = remove_outliers(df, numeric_columns)

    df = df.drop_duplicates()

    # Xuất ra file clean
    df.to_csv(f'Clean_Quan_5.csv', encoding="utf-8-sig", header=True, index=None)


def load():
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_file(
        filename='Clean_Quan_5.csv',
        key='Clean_Quan_5.csv',
        bucket_name='etl-bucket',
        replace=True
    )
    logging.info("File Clean_Quan_5.csv has been pushed to S3!", )

with DAG(
    default_args = default_args,
    dag_id = 'example',
    start_date = datetime(2025,4,24),
    schedule_interval = '@once'
) as dag:
    transform = PythonOperator(
        task_id = "tranform",
        python_callable = transform
    )

    load = PythonOperator(
        task_id = "load",
        python_callable = load
    )


    transform >> load