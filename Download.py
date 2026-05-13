import os
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import socket

print(socket.gethostbyname("cds.climate.copernicus.eu"))


# ===============================
# Mapping biến ERA5 (CDS request name <-> NC file name)
# ===============================

# Tên biến dùng để request từ CDS
CDS_VAR_MAP = {
    "t": "temperature",
    "z": "geopotential",
    "u": "u_component_of_wind",
    "v": "v_component_of_wind",
    "q": "specific_humidity",
    "r": "relative_humidity",
    "vo": "vorticity",
    "sst": "sea_surface_temperature"
}

# Tên biến thực tế trong file NetCDF
NC_VAR_MAP = {
    "temperature": "t",
    "geopotential": "z",
    "u_component_of_wind": "u",
    "v_component_of_wind": "v",
    "specific_humidity": "q",
    "relative_humidity": "r",
    "vorticity": "vo",
    "sea_surface_temperature": "sst",

    # nếu nhập trực tiếp dạng ngắn thì giữ nguyên
    "t": "t",
    "z": "z",
    "u": "u",
    "v": "v",
    "q": "q",
    "r": "r",
    "vo": "vo",
    "sst": "sst"
}


# ===============================
# Tên file xuất ra Excel
# ===============================
OUTPUT_NAME = {
    "z": "geopotential",
    "t": "temperature",
    "u": "Wind_U",
    "v": "Wind_V",
    "q": "SpecificHumidity",
    "r": "RelativeHumidity",
    "vo": "Vorticity",
    "sst": "SST",

    # nếu user dùng tên dài thì vẫn hỗ trợ
    "u_component_of_wind": "Wind_U",
    "v_component_of_wind": "Wind_V",
    "specific_humidity": "SpecificHumidity",
    "relative_humidity": "RelativeHumidity",
    "vorticity": "Vorticity",
    "sea_surface_temperature": "SST"
}


# ===============================
# Các biến thuộc SINGLE LEVELS (không có pressure_level)
# ===============================
SINGLE_LEVEL_VARS = {
    "sea_surface_temperature",
    "sst"
}


# ===============================
# Hàm tạo list ngày
# ===============================
def date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    while start <= end:
        dates.append(start)
        start += timedelta(days=1)

    return dates


# ===============================
# Kiểm tra file Excel đã tồn tại chưa
# ===============================
def check_exists(base_dir, date_obj, hour, var_name, pressure_level):
    date_folder = date_obj.strftime("%d_%m_%Y")
    hour_folder = f"{hour:02d}_00"

    var_label = var_name

    if pressure_level is not None and var_name not in SINGLE_LEVEL_VARS:
        filename = f"{var_label}_{pressure_level}hPa.xlsx"
    else:
        filename = f"{var_label}.xlsx"

    file_path = os.path.join(base_dir, date_folder, hour_folder, filename)
    return os.path.exists(file_path), file_path


# ===============================
# Tải ERA5 từ CDS (SAFE DOWNLOAD)
# ===============================
def download_era5_nc(output_nc, date_obj, hour, var_name, pressure_level=None):
    c = cdsapi.Client()

    # đổi var_name sang tên chuẩn để request CDS
    cds_var = CDS_VAR_MAP.get(var_name, var_name)

    # Nếu biến thuộc single-levels thì bỏ pressure_level
    if cds_var in SINGLE_LEVEL_VARS or var_name in SINGLE_LEVEL_VARS:
        dataset = "reanalysis-era5-single-levels"
        pressure_level = None
    else:
        dataset = "reanalysis-era5-pressure-levels"

    request = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": cds_var,
        "year": date_obj.strftime("%Y"),
        "month": date_obj.strftime("%m"),
        "day": date_obj.strftime("%d"),
        "time": f"{hour:02d}:00",
    }

    if pressure_level is not None:
        request["pressure_level"] = [str(pressure_level)]

    # SAFE DOWNLOAD: tải vào file tạm
    temp_file = output_nc + ".part"

    # Nếu file cuối đã tồn tại -> skip
    if os.path.exists(output_nc) and os.path.getsize(output_nc) > 0:
        print("File NC đã tồn tại -> skip:", output_nc)
        return

    # Nếu file tải dở tồn tại -> xóa để tải lại
    if os.path.exists(temp_file):
        print("Phát hiện file tải dở (.part) -> xóa và tải lại:", temp_file)
        os.remove(temp_file)

    print(f"Đang tải: {dataset} | {cds_var} | {date_obj.date()} {hour:02d}:00")

    try:
        c.retrieve(dataset, request, temp_file)

        # tải xong -> đổi tên thành file chính thức
        os.replace(temp_file, output_nc)

        print("Tải xong:", output_nc)

    except Exception as e:
        print("Lỗi tải dữ liệu:", e)

        if os.path.exists(temp_file):
            print("File tải dở vẫn còn:", temp_file)


# ==========================================================
# HÀM TRÍCH XUẤT GRID 2D
# ==========================================================
def extract_era5_grid(nc_file, var_name,
                      lat_low_bound, lat_up_bound,
                      long_low_bound, long_up_bound):
    """
    OUTPUT:
        grid_2d: numpy array 2D (lat x lon)
    """

    ds = xr.open_dataset(nc_file)

    # đổi var_name sang tên biến thật trong file nc
    nc_var = NC_VAR_MAP.get(var_name, var_name)

    if nc_var not in ds.data_vars:
        raise KeyError(f"Biến '{nc_var}' không có trong file NC. "
                       f"Data vars hiện có: {list(ds.data_vars.keys())}")

    data_var = ds[nc_var]

    # ERA5 có thể có time hoặc valid_time
    if "valid_time" in data_var.dims:
        data_var = data_var.isel(valid_time=0)

    if "time" in data_var.dims:
        data_var = data_var.isel(time=0)

    # nếu có level (pressure level)
    if "pressure_level" in data_var.dims:
        data_var = data_var.isel(pressure_level=0)

    # --------------------------
    # Cắt latitude (ERA5 thường giảm dần)
    # --------------------------
    lat0 = ds["latitude"].values[0]
    lat_last = ds["latitude"].values[-1]

    if lat0 > lat_last:
        data_var = data_var.sel(latitude=slice(lat_up_bound, lat_low_bound))
    else:
        data_var = data_var.sel(latitude=slice(lat_low_bound, lat_up_bound))

    # --------------------------
    # Convert longitude [-180,180] -> [0,360] nếu dataset dùng 0-360
    # --------------------------
    lon_data = ds["longitude"].values

    lon_low = long_low_bound
    lon_up = long_up_bound

    if lon_data.min() >= 0:
        if lon_low < 0:
            lon_low = lon_low % 360
        if lon_up < 0:
            lon_up = lon_up % 360

    # --------------------------
    # Cắt longitude
    # --------------------------
    if lon_low <= lon_up:
        data_var = data_var.sel(longitude=slice(lon_low, lon_up))
    else:
        part1 = data_var.sel(longitude=slice(lon_low, 360))
        part2 = data_var.sel(longitude=slice(0, lon_up))
        data_var = xr.concat([part1, part2], dim="longitude")

    grid_2d = data_var.values

    ds.close()
    return grid_2d


# ===============================
# Xử lý file NetCDF -> Excel dạng lưới
# ===============================
def nc_to_grid_excel(nc_file, excel_file, var_name):
    # vùng cắt dữ liệu
    lat_low_bound = 7
    lat_up_bound = 83
    long_low_bound = 0
    long_up_bound = 137

    grid_2d = extract_era5_grid(
        nc_file, var_name,
        lat_low_bound, lat_up_bound,
        long_low_bound, long_up_bound
    )

    ds = xr.open_dataset(nc_file)

    nc_var = NC_VAR_MAP.get(var_name, var_name)
    data_var = ds[nc_var]

    if "valid_time" in data_var.dims:
        data_var = data_var.isel(valid_time=0)
    if "time" in data_var.dims:
        data_var = data_var.isel(time=0)
    if "pressure_level" in data_var.dims:
        data_var = data_var.isel(pressure_level=0)

    # cắt giống extract_era5_grid
    lat0 = ds["latitude"].values[0]
    lat_last = ds["latitude"].values[-1]

    if lat0 > lat_last:
        data_var = data_var.sel(latitude=slice(lat_up_bound, lat_low_bound))
    else:
        data_var = data_var.sel(latitude=slice(lat_low_bound, lat_up_bound))

    lon_data = ds["longitude"].values
    lon_low = long_low_bound
    lon_up = long_up_bound

    if lon_data.min() >= 0:
        if lon_low < 0:
            lon_low = lon_low % 360
        if lon_up < 0:
            lon_up = lon_up % 360

    if lon_low <= lon_up:
        data_var = data_var.sel(longitude=slice(lon_low, lon_up))
    else:
        part1 = data_var.sel(longitude=slice(lon_low, 360))
        part2 = data_var.sel(longitude=slice(0, lon_up))
        data_var = xr.concat([part1, part2], dim="longitude")

    lat = data_var["latitude"].values
    lon = data_var["longitude"].values

    df = pd.DataFrame(grid_2d, index=lat, columns=lon)

    os.makedirs(os.path.dirname(excel_file), exist_ok=True)
    df.to_excel(excel_file)

    ds.close()
    print("Đã lưu Excel:", excel_file)
    print("Kích thước grid:", df.shape)


# ===============================
# Pipeline chính
# ===============================
def era5_pipeline(
        base_dir="ERA5",
        start_date="2026-04-01",
        end_date="2026-04-02",
        hours=[6, 12, 18, 0],
        variables=["t"],
        pressure_level=None
):
    dates = date_range(start_date, end_date)

    for date_obj in dates:
        for hour in hours:
            for var_name in variables:

                exists, excel_path = check_exists(base_dir, date_obj, hour, var_name, pressure_level)

                if exists:
                    print("Đã có dữ liệu -> skip:", excel_path)
                    continue

                temp_dir = os.path.join(base_dir, "_raw_nc")
                os.makedirs(temp_dir, exist_ok=True)

                nc_filename = f"{var_name}_{date_obj.strftime('%Y%m%d')}_{hour:02d}00"

                if pressure_level is not None and var_name not in SINGLE_LEVEL_VARS:
                    nc_filename += f"_{pressure_level}hPa"

                nc_filename += ".nc"

                nc_path = os.path.join(temp_dir, nc_filename)

                # tải nc (safe download)
                download_era5_nc(nc_path, date_obj, hour, var_name, pressure_level)

                # convert sang excel
                nc_to_grid_excel(nc_path, excel_path, var_name)

                # xóa nc sau khi convert để tiết kiệm dung lượng
                if os.path.exists(nc_path):
                    os.remove(nc_path)
                    print("Đã xóa file NC:", nc_path)


# ===============================
# Ví dụ chạy
# ===============================
if __name__ == "__main__":
    era5_pipeline(
        base_dir="ERA5",
        start_date="1940-01-01",
        end_date="2000-12-31",
        hours=[0,6,12,18],
        variables=[
            "t", "z",
            "q", "r", "vo",
            "u", "v",
            "sst"
        ],
        pressure_level=750
    )
