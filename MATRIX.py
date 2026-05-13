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
    "ap_suat": "geopotential",
    "nhiet_do": "temperature",
    "u_gio": "u_component_of_wind",
    "v_gio": "v_component_of_wind"
}

# Tên file xuất ra Excel
OUTPUT_NAME = {
    "z": "geopotential",
    "t": "temperature",
    "u_component_of_wind": "Gio_U",
    "v_component_of_wind": "Gio_V"
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
# Kiểm tra file đã tồn tại chưa
# ===============================
def check_exists(base_dir, date_obj, hour, var_name, pressure_level):
    date_folder = date_obj.strftime("%d_%m_%Y")
    hour_folder = f"{hour:02d}_00"

    var_label = OUTPUT_NAME[var_name]

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

    if pressure_level is not None:
        dataset = "reanalysis-era5-pressure-levels"
        request["pressure_level"] = [str(pressure_level)]
    else:
        dataset = "reanalysis-era5-single-levels"

    print(f"Đang tải: {dataset} | {var_name} | {date_obj.date()} {hour:02d}:00")

    c.retrieve(dataset, request, output_nc)
    print("Tải xong:", output_nc)


# ==========================================================
# (MỚI) HÀM TRÍCH XUẤT GRID 2D ĐÚNG THEO YÊU CẦU BÀI TOÁN
# ==========================================================
def extract_era5_grid(nc_file, var_name,
                      lat_low_bound, lat_up_bound,
                      long_low_bound, long_up_bound,
                      pressure_level=None):
    """
    INPUT:
        nc_file: file ERA5 netCDF
        var_name: biến ('t', 'z', 'u_component_of_wind', ...)
        lat_low_bound, lat_up_bound
        long_low_bound, long_up_bound
        pressure_level: None hoặc mức hPa

    OUTPUT:
        grid_2d: numpy array 2D (lat x lon)
    """

    ds = xr.open_dataset(nc_file)
    data_var = ds[var_name]

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
        # trường hợp vùng bị cắt qua kinh tuyến 0
        part1 = data_var.sel(longitude=slice(lon_low, 360))
        part2 = data_var.sel(longitude=slice(0, lon_up))
        data_var = xr.concat([part1, part2], dim="longitude")

    grid_2d = data_var.values

    ds.close()
    return grid_2d


# ===============================
# Xử lý file NetCDF -> Excel dạng lưới
# (SỬ DỤNG HÀM extract_era5_grid)
# ===============================
def nc_to_grid_excel(nc_file, excel_file, var_name, lat_low_bound=18,):
    # bạn có thể chỉnh vùng cắt tại đây
    lat_low_bound = 18
    lat_up_bound = 24
    long_low_bound = 102
    long_up_bound = 110

    grid_2d = extract_era5_grid(
        nc_file, var_name,
        lat_low_bound, lat_up_bound,
        long_low_bound, long_up_bound
    )

    # mở dataset lại để lấy lat/lon tương ứng (cho đúng index/columns)
    ds = xr.open_dataset(nc_file)
    data_var = ds[var_name]

    if "valid_time" in data_var.dims:
        data_var = data_var.isel(valid_time=0)
    if "time" in data_var.dims:
        data_var = data_var.isel(time=0)
    if "pressure_level" in data_var.dims:
        data_var = data_var.isel(pressure_level=0)

    # cắt giống hệt như extract_era5_grid để lấy lat/lon
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
        variables=["temperature"],
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
                if pressure_level is not None:
                    nc_filename += f"_{pressure_level}hPa"
                nc_filename += ".nc"

                nc_path = os.path.join(temp_dir, nc_filename)

                download_era5_nc(nc_path, date_obj, hour, var_name, pressure_level)

                nc_to_grid_excel(nc_path, excel_path, var_name)


# ===============================
# Ví dụ chạy
# ===============================
if __name__ == "__main__":
    era5_pipeline(
        base_dir="ERA5",
        start_date="2000-04-01",
        end_date="2000-04-02",
        hours=[6, 12, 18, 0],
        variables=["t", "z"],
        pressure_level=750
    )