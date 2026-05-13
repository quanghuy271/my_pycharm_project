import xarray as xr
import numpy as np

def extract_era5_2d(
        nc_file,
        year, month, day, hour,
        var_name,
        lat_low_bound, lat_up_bound,
        long_low_bound, long_up_bound,
        pressure_level=None
):
    """
    INPUT:
        nc_file : đường dẫn file .nc
        year, month, day, hour : thời gian cần lấy
        var_name : tên biến trong file nc (vd: 't', 'z', 'q', 'r', 'vo', 'u', 'v', 'sst')
        pressure_level : nếu là pressure-level variable
        lat_low_bound, lat_up_bound : giới hạn vĩ độ
        long_low_bound, long_up_bound : giới hạn kinh độ

    OUTPUT:
        grid_2d : numpy array (lat x lon)
    """

    ds = xr.open_dataset(nc_file)

    # ============================
    # 1. Chọn biến
    # ============================
    data_var = ds[var_name]

    # ============================
    # 2. Chọn thời gian
    # ============================
    if "time" in data_var.dims:
        data_var = data_var.sel(
            time=np.datetime64(f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00")
        )

    if "valid_time" in data_var.dims:
        data_var = data_var.sel(
            valid_time=np.datetime64(f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00")
        )

    # ============================
    # 3. Chọn pressure level (nếu có)
    # ============================
    if pressure_level is not None and "pressure_level" in data_var.dims:
        data_var = data_var.sel(pressure_level=pressure_level)

    # ============================
    # 4. Xử lý latitude
    # ============================
    lat_values = ds["latitude"].values

    if lat_values[0] > lat_values[-1]:
        # lat giảm dần
        data_var = data_var.sel(latitude=slice(lat_up_bound, lat_low_bound))
    else:
        data_var = data_var.sel(latitude=slice(lat_low_bound, lat_up_bound))

    # ============================
    # 5. Xử lý longitude
    # ============================
    lon_values = ds["longitude"].values

    lon_low = long_low_bound
    lon_up = long_up_bound

    # Nếu dữ liệu dùng 0-360
    if lon_values.min() >= 0:
        if lon_low < 0:
            lon_low = lon_low % 360
        if lon_up < 0:
            lon_up = lon_up % 360

    # Cắt bình thường
    if lon_low <= lon_up:
        data_var = data_var.sel(longitude=slice(lon_low, lon_up))
    else:
        # Trường hợp cắt qua kinh tuyến 0
        part1 = data_var.sel(longitude=slice(lon_low, 360))
        part2 = data_var.sel(longitude=slice(0, lon_up))
        data_var = xr.concat([part1, part2], dim="longitude")

    # ============================
    # 6. Xuất mảng 2D
    # ============================
    grid_2d = data_var.values

    ds.close()

    return grid_2d
grid = extract_era5_2d(
    nc_file="t_20000401_0600_750hPa.nc",
    year=2000, month=4, day=1, hour=6,
    var_name="t",
    lat_low_bound=18,
    lat_up_bound=24,
    long_low_bound=102,
    long_up_bound=110,
    pressure_level=750
)

print(grid.shape)