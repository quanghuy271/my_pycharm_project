import os
import glob

base_dir = "ERA5"

# ==========================
# XÓA TOÀN BỘ FILE .nc, .part, .xlsx
# ==========================
for f in glob.glob(os.path.join(base_dir, "**", "*.nc"), recursive=True):
    os.remove(f)

for f in glob.glob(os.path.join(base_dir, "**", "*.part"), recursive=True):
    os.remove(f)

for f in glob.glob(os.path.join(base_dir, "**", "*.xlsx"), recursive=True):
    os.remove(f)

print("Đã xóa toàn bộ file .nc, .part và .xlsx trong ERA5")

# ==========================
# XÓA THƯ MỤC RỖNG
# ==========================
for root, dirs, files in os.walk(base_dir, topdown=False):
    if not os.listdir(root):
        os.rmdir(root)

print("Đã xóa toàn bộ thư mục rỗng trong ERA5")