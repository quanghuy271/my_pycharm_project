import os
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import socket
print(socket.gethostbyname("cds.climate.copernicus.eu"))


# ===============================
# Cấu hình mapping biến ERA5
# ===============================
ERA5_VARIABLES = {
    "ap_suat": "geopotential",   # dùng geopotential để suy ra độ cao hoặc phân tích áp suất
    "nhiet_do": "temperature",
    "u_gio": "u_component_of_wind",
    "v_gio": "v_component_of_wind"
}

# Tên file xuất ra Excel
OUTPUT_NAME = {
    "z" : "geopotential",
    "t" : "temperature",
    "u_component_of_wind": "Gio_U",
    "v_component_of_wind": "Gio_V"
}


# ===============================
# Hàm tạo list ngày
# ===============================
def date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d") #strptime = string parse time (phân tích chuỗi thành thời gian)

    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    while start <= end:
        dates.append(start)
        start += timedelta(days=1)

    return dates


# ===============================
# Kiểm tra file đã tồn tại chưa
# ===============================
def check_exists(base_dir, date_obj, hour, var_name, pressure_level):
    date_folder = date_obj.strftime("%d_%m_%Y")
    hour_folder = f"{hour:02d}_00"

    var_label = OUTPUT_NAME[var_name]

    # Nếu có pressure_level thì thêm vào tên file
    if pressure_level is not None:
        filename = f"{var_label}_{pressure_level}hPa.xlsx"
    else:
        filename = f"{var_label}.xlsx"

    file_path = os.path.join(base_dir, date_folder, hour_folder, filename)
    return os.path.exists(file_path), file_path


# ===============================
# Tải ERA5 từ CDS
# ===============================
def download_era5_nc(output_nc, date_obj, hour, var_name, pressure_level=None):
    c = cdsapi.Client()

    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": var_name,
        "year": date_obj.strftime("%Y"),
        "month": date_obj.strftime("%m"),
        "day": date_obj.strftime("%d"),
        "time": f"{hour:02d}:00",
    }

    # Nếu có level -> pressure level dataset
    if pressure_level is not None:
        dataset = "reanalysis-era5-pressure-levels"
        request["pressure_level"] = [str(pressure_level)]   #chon pressure level cu the
    else:
        dataset = "reanalysis-era5-single-levels"

    print(f"Đang tải: {dataset} | {var_name} | {date_obj.date()} {hour:02d}:00")

    c.retrieve(dataset, request, output_nc)
    print("Tải xong:", output_nc)


# ===============================
# Xử lý file NetCDF -> Excel dạng lưới
# ===============================
def nc_to_grid_excel(nc_file, excel_file, var_name):
    ds = xr.open_dataset(nc_file)

    # chọn biến chính
    data_var = ds[var_name]

    # ERA5 thường có dims: time, latitude, longitude
    # lấy thời điểm duy nhất
    if "valid_time" in data_var.dims:
        data_var = data_var.isel(valid_time=0)

    # nếu có level (pressure level)
    if "pressure_level" in data_var.dims:
        data_var = data_var.isel(pressure_level=0)

    lat = data_var["latitude"].values
    lon = data_var["longitude"].values
    values = data_var.values

    # tạo dataframe dạng lưới lat x lon
    df = pd.DataFrame(values, index=lat, columns=lon)

    # ghi ra Excel
    os.makedirs(os.path.dirname(excel_file), exist_ok=True)
    df.to_excel(excel_file)

    ds.close()
    print("Đã lưu Excel:", excel_file)


# ===============================
# Pipeline chính
# ===============================
def era5_pipeline(
        base_dir="ERA5",
        start_date="2026-05-01",
        end_date="2026-05-03",
        hours=[6, 12, 18, 0],
        variables=["temperature"],
        pressure_level=None
):
    """
    base_dir: thư mục database (ERA5/)
    start_date/end_date: YYYY-MM-DD
    hours: list giờ [6,12,18,0]
    variables: list biến ERA5 (temperature, geopotential,...)
    pressure_level: None hoặc mức hPa (vd 750)
    """

    dates = date_range(start_date, end_date)

    for date_obj in dates:
        for hour in hours:
            for var_name in variables:

                # kiểm tra đã tồn tại chưa
                exists, excel_path = check_exists(base_dir, date_obj, hour, var_name, pressure_level)

                if exists:
                    print("Đã có dữ liệu -> skip:", excel_path)
                    continue

                # nếu chưa có -> tải netcdf
                temp_dir = os.path.join(base_dir, "_raw_nc")
                os.makedirs(temp_dir, exist_ok=True)

                nc_filename = f"{var_name}_{date_obj.strftime('%Y%m%d')}_{hour:02d}00"
                if pressure_level is not None:
                    nc_filename += f"_{pressure_level}hPa"
                nc_filename += ".nc"

                nc_path = os.path.join(temp_dir, nc_filename)

                download_era5_nc(nc_path, date_obj, hour, var_name, pressure_level)

                # xử lý -> lưu excel dạng lưới
                nc_to_grid_excel(nc_path, excel_path, var_name)

                # xóa file raw nc nếu muốn tiết kiệm bộ nhớ
                # os.remove(nc_path)


# ===============================
# Ví dụ chạy
# ===============================
if __name__ == "__main__":
    # Ví dụ: lưu từ 01/05/2026 -> 03/05/2026
    # giờ 6h,12h,18h,24h(0h)
    # lưu nhiệt độ + geopotential tại level 750hPa

    era5_pipeline(
        base_dir="ERA5",
        start_date="2026-05-01",
        end_date="2026-05-03",
        hours=[6, 12, 18, 0],
        variables=["t", "z"],
        pressure_level=750
    )

