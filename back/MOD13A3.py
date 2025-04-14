import os
import numpy as np
import rasterio
import xarray as xr
import rioxarray as rxr
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import box
import glob
from tqdm import tqdm

# ========== 全局配置 ==========
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 300

# ========== 路径配置 ==========
PROVINCE_SHP = r"D:\\data\\shp\\SHP文件\\中国_省.shp"
CITY_SHP = r"D:\\data\\shp\\SHP文件\\中国_市.shp"

# 输入和输出目录
NDVI_DIR = r"D:\download\百度网盘\干旱灾害可视化遥感数据\MOD13A3\202401\processed_ndvi"  # 修改为您的NDVI文件目录
OUTPUT_DIR = r"D:\download\百度网盘\干旱灾害可视化遥感数据\MOD13A3\202401\处理"  # 修改为您想保存结果的目录


def create_jet_colormap():
    """创建256级jet颜色表（兼容Matplotlib 3.7+）"""
    cmap = {}
    jet = plt.colormaps['jet'].resampled(256)

    for i in range(256):
        r, g, b, _ = jet(i / 255)
        cmap[i] = (
            int(r * 255),
            int(g * 255),
            int(b * 255),
            255
        )
    cmap[0] = (0, 0, 0, 0)  # 设置0为透明（用于nodata）
    return cmap


def parse_ndvi_date(filename):
    """从NDVI文件名中解析日期"""
    # 示例: scaled_2024001_NDVI.tif -> 2024年第1天
    date_part = os.path.basename(filename).split('_')[1]
    year = int(date_part[:4])
    doy = int(date_part[4:])

    from datetime import datetime, timedelta
    base_date = datetime(year, 1, 1)
    target_date = base_date + timedelta(days=doy - 1)
    return target_date


def process_ndvi_by_region(ndvi_files, output_dir):
    """处理NDVI数据并按省市范围裁剪"""
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "province"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "city"), exist_ok=True)

    # 加载省市边界数据
    gdf_prov = gpd.read_file(PROVINCE_SHP).to_crs("EPSG:4326")
    gdf_city = gpd.read_file(CITY_SHP).to_crs("EPSG:4326")

    # 加载NDVI数据并创建时间序列数据集
    print("加载NDVI数据...")
    da_list = []

    for f in tqdm(ndvi_files):
        try:
            # 读取栅格数据
            with rxr.open_rasterio(f) as src:
                da = src.squeeze()

                # 确保使用正确的CRS
                if not da.rio.crs:
                    da = da.rio.write_crs("EPSG:4326")

                # 添加时间维度
                date = parse_ndvi_date(f)
                da = da.expand_dims(time=[date]).assign_coords(time=[date])
                da = da.rename("NDVI")

                da_list.append(da)
        except Exception as e:
            print(f"处理文件 {f} 时出错: {e}")

    if not da_list:
        print("没有有效的NDVI文件可处理！")
        return

    # 合并所有时间切片
    print("合并时间序列...")
    ds = xr.concat(da_list, dim='time').sortby('time')

    # 计算数据范围以进行统一色标
    valid_min = float(ds.min(skipna=True).values)
    valid_max = float(ds.max(skipna=True).values)
    print(f"NDVI数据范围: {valid_min:.2f} ~ {valid_max:.2f}")

    # 裁剪省级数据
    print("开始省级数据裁剪...")
    plot_region_data(
        ds, gdf_prov,
        level="province",
        save_root=output_dir,
        name_column="name",
        vmin=valid_min,
        vmax=valid_max
    )

    # 裁剪市级数据
    print("开始市级数据裁剪...")
    plot_region_data(
        ds, gdf_city,
        level="city",
        save_root=output_dir,
        name_column="name",
        vmin=valid_min,
        vmax=valid_max
    )

    print("NDVI数据区域裁剪完成！")


def plot_region_data(ds, gdf, level, save_root, name_column=None, vmin=None, vmax=None):
    """按行政区划裁剪数据并保存"""
    # 创建颜色表
    JET_COLORMAP = create_jet_colormap()

    # 确保坐标系统一
    ds = ds.rio.reproject("EPSG:4326")
    gdf = gdf.to_crs("EPSG:4326")

    # 自动识别行政区字段
    if name_column is None:
        name_fields = [col for col in gdf.columns if 'name' in col.lower()]
        if not name_fields:
            raise ValueError("❌ shapefile 中未找到包含 'name' 的字段")
        name_column = name_fields[0]

    # 遍历每个行政区
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc=f"处理{level}"):
        name = row[name_column]
        if not isinstance(name, str) or name.strip() == "":
            continue

        # 获取行政区几何形状
        region_geom = row['geometry']
        if not region_geom.is_valid:
            print(f"⚠️ {name} 的几何形状无效，跳过")
            continue

        # 检查是否与数据范围相交
        data_bounds = box(*ds.rio.bounds())
        if not data_bounds.intersects(region_geom):
            print(f"⚠️ {name} 不在数据范围内，跳过")
            continue

        # 创建输出目录
        out_dir = os.path.join(save_root, level, name)
        os.makedirs(out_dir, exist_ok=True)

        try:
            # 裁剪区域
            clipped = ds.rio.clip([region_geom], gdf.crs, drop=True)
            if np.isnan(clipped.values).all():
                print(f"⚠️ {name} 裁剪后数据全为NaN，跳过")
                continue

            # 保存每个时间点的数据
            for time_idx in range(len(clipped.time)):
                time_data = clipped.isel(time=time_idx)
                date_str = time_data.time.dt.strftime("%Y%m%d").item()

                # 原始数据路径
                out_path = os.path.join(out_dir, f"{name}_NDVI_{date_str}.tif")
                # 可视化版本路径
                vis_path = os.path.join(out_dir, f"{name}_NDVI_{date_str}_vis.tif")

                # 检查数据有效性
                if np.isnan(time_data.values).all():
                    print(f"⚠️ {name} 在 {date_str} 的数据全为NaN")
                    continue

                # 保存原始数据
                time_data.rio.to_raster(out_path)

                # 准备可视化数据
                data = time_data.values
                valid_mask = ~np.isnan(data)

                # 归一化计算
                data_range = vmax - vmin
                if data_range <= 0:
                    print(f"⚠️ 无效数据范围: {vmin}~{vmax}")
                    continue

                # 缩放到1-255范围，保留0作为nodata
                scaled = np.zeros_like(data, dtype=np.uint8)
                scaled[valid_mask] = 1 + (data[valid_mask] - vmin) / data_range * 254
                scaled = np.clip(scaled, 1, 255).astype(np.uint8)

                # 写入可视化版本
                with rasterio.open(vis_path, 'w',
                                   driver='GTiff',
                                   height=scaled.shape[0],
                                   width=scaled.shape[1],
                                   count=1,
                                   dtype=np.uint8,
                                   crs=time_data.rio.crs,
                                   transform=time_data.rio.transform(),
                                   nodata=0,
                                   compress='lzw'
                                   ) as dst:
                    dst.write(scaled, 1)
                    dst.write_colormap(1, JET_COLORMAP)
                    dst.update_tags(
                        scale_factor=data_range / 255,
                        add_offset=vmin,
                        actual_range=(vmin, vmax)
                    )

        except Exception as e:
            print(f"❌ 区域【{name}】裁剪失败: {e}")


if __name__ == "__main__":
    # 查找所有NDVI文件
    ndvi_files = glob.glob(os.path.join(NDVI_DIR, "scaled_*_NDVI.tif"))
    if not ndvi_files:
        print(f"未找到NDVI文件在目录: {NDVI_DIR}")
    else:
        print(f"找到 {len(ndvi_files)} 个NDVI文件")
        process_ndvi_by_region(ndvi_files, OUTPUT_DIR)