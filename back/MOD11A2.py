import os
import numpy as np
import rasterio
from rasterio.mask import mask
from tqdm import tqdm
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
import rioxarray as rxr
import glob
from matplotlib.animation import FuncAnimation
from matplotlib import rcParams
import geopandas as gpd
from shapely.geometry import box
# 导入pypinyin进行中文转拼音
from pypinyin import lazy_pinyin
# ========== 全局配置 ==========
rcParams['font.sans-serif'] = ['SimHei']
rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 300

SCALE_FACTOR = 0.02
KELVIN_OFFSET = 273.15

# ========== 路径配置 ==========
CHINA_SHP_PATH = r"D:\\data\\shp\\SHP文件\\国界_Project.shp"
PROVINCE_SHP = r"D:\\data\\shp\\SHP文件\\中国_省.shp"
CITY_SHP = r"D:\\data\\shp\\SHP文件\\中国_市.shp"

# ========== 工具函数 ==========
# ========== 工具函数 ==========
def convert_to_pinyin(chinese_str):
    """将中文名称转换为拼音"""
    return ''.join(lazy_pinyin(chinese_str)).lower()

def create_directories(base_path):
    dirs = [
        os.path.join(base_path, "processed_masked", "Day"),
        os.path.join(base_path, "processed_masked", "Night"),
        os.path.join(base_path, "processed_mean"),
        os.path.join(base_path, "plots")
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def parse_julian_date(filename):
    date_str = os.path.basename(filename).split('_')[-1].split('.')[0]
    year, doy = int(date_str[:4]), int(date_str[4:])
    return pd.Timestamp(year=year, month=1, day=1) + pd.DateOffset(days=doy - 1)

def mask_lst_with_qc(lst_path, qc_path, output_dir):
    with rasterio.open(lst_path) as lst_src:
        lst_data = lst_src.read(1).astype(np.float32) * SCALE_FACTOR - KELVIN_OFFSET
        profile = lst_src.profile.copy()
        profile.update(dtype='float32', nodata=np.nan)

    with rasterio.open(qc_path) as qc_src:
        qc_data = qc_src.read(1)

    qc_mask = (qc_data & 0b00000011) != 0
    lst_masked = np.where(qc_mask, np.nan, lst_data)

    output_path = os.path.join(output_dir, os.path.basename(lst_path))
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(lst_masked, 1)

def batch_process_qc_masking(lst_dir, qc_dir, output_dir):
    lst_files = sorted([f for f in os.listdir(lst_dir) if f.startswith('LST_') and f.endswith('.tif')])
    for lst_file in tqdm(lst_files, desc=f"处理 {lst_dir}"):
        date_tag = lst_file.split('_')[-1].split('.')[0]
        qc_file = f"QC_{'_'.join(lst_file.split('_')[1:-1])}_{date_tag}.tif"
        qc_path = os.path.join(qc_dir, qc_file)
        if os.path.exists(qc_path):
            mask_lst_with_qc(os.path.join(lst_dir, lst_file), qc_path, output_dir)
        else:
            print(f"❌ 缺失 QC 文件: {qc_file}")

def calculate_daily_mean(base_path):
    day_dir = os.path.join(base_path, "processed_masked", "Day")
    night_dir = os.path.join(base_path, "processed_masked", "Night")
    mean_dir = os.path.join(base_path, "processed_mean")
    os.makedirs(mean_dir, exist_ok=True)

    day_files = sorted(glob.glob(os.path.join(day_dir, "LST_Day_*.tif")))
    for day_path in tqdm(day_files, desc="计算昼夜平均"):
        date_tag = os.path.basename(day_path).split('_')[-1].split('.')[0]
        night_path = os.path.join(night_dir, f"LST_Night_{date_tag}.tif")
        if not os.path.exists(night_path):
            print(f"⚠️ 缺少夜间文件: {date_tag}")
            continue

        with rasterio.open(day_path) as d_src, rasterio.open(night_path) as n_src:
            d_data, n_data = d_src.read(1), n_src.read(1)
            profile = d_src.profile

        if np.all(np.isnan(d_data)) or np.all(np.isnan(n_data)):
            continue

        mean_data = np.nanmean([d_data, n_data], axis=0)
        out_path = os.path.join(mean_dir, f"LST_Mean_{date_tag}.tif")
        with rasterio.open(out_path, 'w', **profile) as dst:
            dst.write(mean_data, 1)

def plot_region_mean(ds, gdf, level, save_root, title_prefix="", name_column=None, vmin=None, vmax=None):
    # 在函数开头创建颜色表
    JET_COLORMAP = create_jet_colormap()  # <--- 新增

    # 坐标系统一
    ds = ds.rio.reproject("EPSG:4326")
    gdf = gdf.to_crs(ds.rio.crs)

    # 自动识别行政区字段
    if name_column is None:
        name_fields = [col for col in gdf.columns if 'name' in col.lower()]
        if not name_fields:
            raise ValueError("❌ shapefile 中未找到包含 'name' 的字段")
        name_column = name_fields[0]

    for idx, row in gdf.iterrows():
        name = row[name_column]
        if not isinstance(name, str) or name.strip() == "":
            continue

        region_geom = row['geometry']
        if not region_geom.is_valid:
            continue

        data_bounds = box(*ds.rio.bounds())
        if not data_bounds.intersects(region_geom):
            continue

        out_dir = os.path.join(save_root, level, name)
        os.makedirs(out_dir, exist_ok=True)

        try:
            # 裁剪区域
            clipped = ds.rio.clip([region_geom], gdf.crs, drop=True)
            if np.isnan(clipped.values).all():
                continue

            # 计算每月平均并保存为TIFF
            mean_month = clipped.groupby('time.month').mean()
            # ===== 修复循环缩进问题 =====
            for m in range(1, 13):
                if m not in mean_month['month'].values:
                    print(f"⚠️ {name} 缺少月份{m}数据")
                    continue
                # 将中文名称转换为拼音
                pinyin_name = convert_to_pinyin(name)

                # 原始数据路径
                out_path = os.path.join(out_dir, f"{pinyin_name}_Tep_month{m}.tif")
                # 可视化版本路径
                vis_path = os.path.join(out_dir, f"{pinyin_name}_Tep_month{m}_vis.tif")
                # # 原始数据路径
                # out_path = os.path.join(out_dir, f"{name}_month{m}.tif")
                # # 可视化版本路径
                # vis_path = os.path.join(out_dir, f"{name}_month{m}_vis.tif")

                # ===== 新增数据有效性检查 =====
                month_data = mean_month.sel(month=m)
                if np.isnan(month_data.values).all():
                    print(f"⚠️ {name} {m}月数据全为NaN")
                    continue

                # 保存原始数据
                month_data.rio.to_raster(out_path)
                print(f"生成 {out_path}")  # 调试输出

                # ===== 数据转换流程 =====
                data = month_data.values
                valid_mask = ~np.isnan(data)

                # 归一化计算（修复除零错误）
                data_range = vmax - vmin
                if data_range <= 0:
                    print(f"⚠️ 无效数据范围: {vmin}~{vmax}")
                    continue

                scaled = np.zeros_like(data, dtype=np.uint8)
                scaled[valid_mask] = 1 + (data[valid_mask] - vmin) / data_range * 254
                scaled = np.clip(scaled, 1, 255).astype(np.uint8)

                # ===== 写入可视化版本 =====
                with rasterio.open(vis_path, 'w',
                                   driver='GTiff',
                                   height=scaled.shape[0],
                                   width=scaled.shape[1],
                                   count=1,
                                   dtype=np.uint8,
                                   crs=month_data.rio.crs,
                                   transform=month_data.rio.transform(),
                                   nodata=0,
                                   compress='lzw'
                                   ) as dst:
                    dst.write(scaled, 1)
                    dst.write_colormap(1, JET_COLORMAP)  # <--- 使用颜色表
                    dst.update_tags(
                        scale_factor=data_range / 255,
                        add_offset=vmin,
                        actual_range=(vmin, vmax)
                    )
                print(f"生成 {vis_path}")  # 调试输出
        except Exception as e:
            print(f"❌ 区域【{name}】裁剪失败: {e}")

def temporal_analysis(base_path):
    mean_files = glob.glob(os.path.join(base_path, "processed_mean", "LST_Mean_*.tif"))
    da_list = []

    for f in mean_files:
        with rasterio.open(f) as src:
            crs = src.crs
            print(f"📡 文件 {os.path.basename(f)} 的CRS: {crs}")

        da = rxr.open_rasterio(f).rio.write_crs(crs)
        da = da.rio.reproject("EPSG:4326")

        date = parse_julian_date(f)
        da = da.assign_coords(time=date).isel(band=0).drop_vars('band').rename('LST').load()
        da_list.append(da)

    ds = xr.concat(da_list, dim='time').sortby('time')
    ds.rio.write_crs("EPSG:4326", inplace=True)

    # ✅ 裁剪到中国区域
    china_bounds = [73, 18, 135, 54]
    ds = ds.rio.clip_box(*china_bounds)

    # ✅ 计算统一色标范围
    global_min = float(ds.min(skipna=True).values)
    global_max = float(ds.max(skipna=True).values)
    print(f"🎯 全部数据温度范围: {global_min:.2f}°C ~ {global_max:.2f}°C")

    # ✅ 全国月均图（下采样+统一色标）
    monthly_stats = ds.groupby('time.month').mean(dim='time')
    ds_downsampled = monthly_stats.coarsen(x=5, y=5, boundary='trim').mean()

    os.makedirs(os.path.join(base_path, "plots"), exist_ok=True)

    for m in range(1, 13):
        if m not in ds_downsampled['month']:
            continue
        out_path = os.path.join(base_path, "plots", f"china_month{m}.tif")
        ds_downsampled.sel(month=m).rio.to_raster(out_path)

    # ✅ 加载 shapefile 并绘图
    gdf_prov = gpd.read_file(PROVINCE_SHP).to_crs("EPSG:4326")
    gdf_city = gpd.read_file(CITY_SHP).to_crs("EPSG:4326")

    plot_region_mean(
        ds, gdf_prov,
        level="province",
        save_root=os.path.join(base_path, "plots"),
        title_prefix="省级 ",
        name_column="name",
        vmin=global_min,
        vmax=global_max
    )

    plot_region_mean(
        ds, gdf_city,
        level="city",
        save_root=os.path.join(base_path, "plots"),
        title_prefix="市级 ",
        name_column="name",
        vmin=global_min,
        vmax=global_max
    )

    # ✅ GIF 动图（下采样）
    ds_anim = ds.coarsen(x=5, y=5, boundary='trim').mean()
    fig, ax = plt.subplots(figsize=(10, 6))

    def update(frame):
        ax.clear()
        ds_anim.isel(time=frame).plot(
            ax=ax,
            cmap='jet',
            vmin=global_min,
            vmax=global_max,
            add_colorbar=False,
            robust=True
        )
        ax.set_title(f"日期: {ds_anim.time[frame].dt.strftime('%Y-%m-%d').item()}", fontsize=14)

    ani = FuncAnimation(fig, update, frames=len(ds_anim.time), interval=500)
    ani.save(os.path.join(base_path, 'lst_animation.gif'), writer='pillow')
    # 创建可视化版本全国数据
    JET_COLORMAP = create_jet_colormap()
    for m in range(1, 13):
        if m not in ds_downsampled['month']:
            continue

        # 原始数据保存路径
        out_path = os.path.join(base_path, "plots", f"china_Tep_month{m}.tif")
        vis_path = os.path.join(base_path, "plots", f"china_Tep_month{m}_vis.tif")
        
        # 保存原始数据
        ds_downsampled.sel(month=m).rio.to_raster(out_path)

        # 准备可视化数据
        data = ds_downsampled.sel(month=m).values
        valid_mask = ~np.isnan(data)

        # 数据归一化
        scaled = np.zeros_like(data, dtype=np.uint8)
        scaled[valid_mask] = 1 + (data[valid_mask] - global_min) / (global_max - global_min) * 254
        scaled = np.clip(scaled, 1, 255).astype(np.uint8)

        # 获取元数据
        with rasterio.open(out_path) as src:
            profile = src.profile.copy()

        # 修改元数据
        vis_profile = profile.copy()
        vis_profile.update(
            dtype=rasterio.uint8,
            nodata=0,
            count=1,
            driver='GTiff',
            compress='lzw'
        )

        # 写入可视化版本
        with rasterio.open(vis_path, 'w', **vis_profile) as dst:
            dst.write(scaled, 1)
            dst.write_colormap(1, JET_COLORMAP)
            dst.update_tags(
                scale_factor=(global_max - global_min) / 255,
                add_offset=global_min,
                colormap='jet',
                actual_range=(global_min, global_max)
            )


# ========== 修正后的颜色表函数 ==========
def create_jet_colormap():
    """创建256级jet颜色表（兼容Matplotlib 3.7+）"""
    cmap = {}
    # 使用新的colormap API
    jet = plt.colormaps['jet'].resampled(256)  # 替代弃用的plt.cm.get_cmap()

    for i in range(256):
        # 获取归一化的颜色值（0-1范围）
        r, g, b, _ = jet(i / 255)  # 输入参数需归一化到0-1
        cmap[i] = (
            int(r * 255),
            int(g * 255),
            int(b * 255),
            255  # 完全不透明
        )
    # 设置0为透明（用于nodata）
    cmap[0] = (0, 0, 0, 0)
    return cmap

def process_mod11_lst(base_dir):
    print(f"🚀 开始处理: {base_dir}")
    create_directories(base_dir)

    batch_process_qc_masking(
        os.path.join(base_dir, "LST", "Day"),
        os.path.join(base_dir, "QC", "Day"),
        os.path.join(base_dir, "processed_masked", "Day")
    )
    batch_process_qc_masking(
        os.path.join(base_dir, "LST", "Night"),
        os.path.join(base_dir, "QC", "Night"),
        os.path.join(base_dir, "processed_masked", "Night")
    )

    calculate_daily_mean(base_dir)
    temporal_analysis(base_dir)
    print(f"✅ 完成: {base_dir}\n")

if __name__ == "__main__":
    root_folder = r"D:\download\百度网盘\干旱灾害可视化遥感数据"
    sub_dirs = sorted([os.path.join(root_folder, d) for d in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, d))])
    for sub_dir in sub_dirs:
        process_mod11_lst(sub_dir)