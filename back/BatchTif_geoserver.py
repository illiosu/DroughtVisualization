import os
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from tqdm import tqdm
import glob
import sys
import time
import logging
import io

# 确保控制台输出编码正确
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("geoserver_publish.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GeoServerPublisher:
    def __init__(self, geoserver_url, username, password):
        """初始化GeoServer发布器"""
        self.geoserver_url = geoserver_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.headers = {'Content-type': 'application/xml'}

        # 测试连接
        self.test_connection()

    def test_connection(self):
        """使用简单的方式测试连接"""
        try:
            # 只测试能否访问管理界面
            response = requests.get(
                f"{self.geoserver_url}/web/",
                auth=self.auth,
                timeout=10
            )
            if response.status_code == 200:
                logger.info("成功连接到GeoServer")
                return True
            else:
                logger.error(f"连接GeoServer失败: HTTP {response.status_code}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"连接GeoServer出错: {str(e)}")
            sys.exit(1)

    def workspace_exists(self, workspace):
        """检查工作区是否存在"""
        response = requests.get(
            f"{self.geoserver_url}/rest/workspaces/{workspace}",
            auth=self.auth
        )
        return response.status_code == 200

    def create_workspace(self, workspace):
        """创建工作区"""
        if self.workspace_exists(workspace):
            logger.info(f"工作区 {workspace} 已存在")
            return True

        xml_data = f"<workspace><name>{workspace}</name></workspace>"
        response = requests.post(
            f"{self.geoserver_url}/rest/workspaces",
            auth=self.auth,
            headers=self.headers,
            data=xml_data
        )

        if response.status_code == 201:
            logger.info(f"创建工作区 {workspace} 成功")
            return True
        else:
            logger.error(f"创建工作区 {workspace} 失败: {response.status_code} - {response.text}")
            return False

    def datastore_exists(self, workspace, datastore):
        """检查数据存储是否存在"""
        response = requests.get(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores/{datastore}",
            auth=self.auth
        )
        return response.status_code == 200

    def create_geotiff_store(self, workspace, store_name, file_path):
        """创建GeoTIFF数据存储"""
        # 检查存储是否已存在
        if self.datastore_exists(workspace, store_name):
            logger.info(f"存储 {store_name} 已存在，将使用现有存储")
            return True

        # 提取绝对路径
        abs_path = os.path.abspath(file_path)

        # 创建coveragestore
        xml_data = f"""
        <coverageStore>
            <name>{store_name}</name>
            <type>GeoTIFF</type>
            <enabled>true</enabled>
            <workspace>{workspace}</workspace>
            <url>file:{abs_path}</url>
        </coverageStore>
        """

        response = requests.post(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores",
            auth=self.auth,
            headers=self.headers,
            data=xml_data
        )

        if response.status_code == 201:
            logger.info(f"创建GeoTIFF存储 {store_name} 成功")
            return True
        else:
            logger.error(f"创建GeoTIFF存储 {store_name} 失败: {response.status_code} - {response.text}")
            return False

    def force_publish_layer(self, workspace, store_name, tif_path):
        """强制发布图层，直接使用GeoServer的自动配置"""
        # 1. 创建coverageStore
        abs_path = os.path.abspath(tif_path)

        # 2. 发送PUT请求，让GeoServer自动配置图层
        headers = {'Content-type': 'text/plain'}
        url = f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores/{store_name}/external.geotiff?configure=all"

        response = requests.put(
            url,
            auth=self.auth,
            headers=headers,
            data=f"file:{abs_path}"
        )

        if response.status_code in (200, 201):
            logger.info(f"成功发布图层: {store_name}")
            return True, store_name
        else:
            logger.error(f"发布图层失败: {response.status_code} - {response.text}")
            return False, None

    def create_layer(self, workspace, store_name, layer_name, title, tif_path):
        """发布图层"""
        # 检查图层是否已存在
        response = requests.get(
            f"{self.geoserver_url}/rest/layers/{workspace}:{layer_name}",
            auth=self.auth
        )

        if response.status_code == 200:
            logger.info(f"图层 {layer_name} 已存在")
            return True, layer_name

        # 步骤1: 创建coverageStore
        if not self.create_geotiff_store(workspace, store_name, tif_path):
            return False, None

        # 步骤2: 获取文件名(不含扩展名)作为coverage名称
        file_basename = os.path.splitext(os.path.basename(tif_path))[0]

        # 步骤3: 配置并发布图层
        try:
            # 直接尝试发布图层
            xml_data = f"""
            <coverage>
                <name>{layer_name}</name>
                <nativeName>{file_basename}</nativeName>
                <title>{title}</title>
                <enabled>true</enabled>
            </coverage>
            """

            response = requests.post(
                f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores/{store_name}/coverages",
                auth=self.auth,
                headers=self.headers,
                data=xml_data
            )

            if response.status_code in (200, 201):
                logger.info(f"发布图层 {layer_name} 成功")
                return True, layer_name
            else:
                logger.warning(f"尝试常规方法发布图层失败: {response.status_code} - {response.text}")
                # logger.info(f"尝试使用强制发布方法...")
                return False, None

        except Exception as e:
            logger.error(f"发布图层 {layer_name} 时出错: {str(e)}")
            return False, None

    def create_layer_group(self, workspace, group_name, layers, styles=None, title=None):
        """创建图层组"""
        # 检查图层组是否已存在
        response = requests.get(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/layergroups/{group_name}",
            auth=self.auth
        )

        if response.status_code == 200:
            logger.info(f"图层组 {group_name} 已存在")
            return True

        # 准备图层组XML
        root = ET.Element("layerGroup")

        name_elem = ET.SubElement(root, "name")
        name_elem.text = group_name

        if title:
            title_elem = ET.SubElement(root, "title")
            title_elem.text = title

        # 添加图层
        for i, layer in enumerate(layers):
            layer_elem = ET.SubElement(root, "publishable")
            publish_elem = ET.SubElement(layer_elem, "published")
            publish_elem.text = "true"
            name_elem = ET.SubElement(layer_elem, "name")
            name_elem.text = f"{workspace}:{layer}"

            if styles and i < len(styles):
                style_elem = ET.SubElement(layer_elem, "style")
                style_elem.text = styles[i]

        # 发送请求
        xml_data = ET.tostring(root, encoding='utf-8')

        response = requests.post(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/layergroups",
            auth=self.auth,
            headers=self.headers,
            data=xml_data
        )

        if response.status_code == 201:
            logger.info(f"创建图层组 {group_name} 成功")
            return True
        else:
            logger.error(f"创建图层组 {group_name} 失败: {response.status_code} - {response.text}")
            return False


def batch_publish_tifs(geoserver_url, username, password, root_dir, workspace_name="ndvi_data", create_groups=False):
    """批量发布TIF文件"""
    publisher = GeoServerPublisher(geoserver_url, username, password)

    # 创建工作区
    if not publisher.create_workspace(workspace_name):
        return

    # 找到所有TIF文件
    vis_tifs = []
    raw_tifs = []

    # 递归查找所有TIF文件
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".tif") and "_vis" in file:
                vis_tifs.append(os.path.join(root, file))
            elif file.endswith(".tif") and "_vis" not in file:
                raw_tifs.append(os.path.join(root, file))

    logger.info(f"找到 {len(vis_tifs)} 个可视化TIF文件和 {len(raw_tifs)} 个原始TIF文件")

    # 发布可视化TIF文件
    region_layers = {}  # 按区域组织图层

    for tif_path in tqdm(vis_tifs, desc="发布可视化TIF"):
        # 从文件路径解析区域和其他信息
        file_name = os.path.basename(tif_path)

        # 尝试提取区域名称、类型和日期
        parts = file_name.split('_')
        if len(parts) >= 3:
            region_name = parts[0]

            # 从路径中获取省份/城市分类
            if "province" in tif_path:
                region_type = "province"
            elif "city" in tif_path:
                region_type = "city"
            else:
                region_type = "unknown"

            # 确定日期或月份信息
            date_info = None
            for part in parts:
                if part.startswith("202"):  # 年份
                    date_info = part
                elif part.startswith("month"):
                    date_info = part

            if not date_info:
                date_info = "unknown_date"

            # 创建图层名称和标题
            store_name = f"{region_name}_{region_type}_store"
            layer_name = f"{region_name}_{date_info}_vis"
            title = f"{region_name} {date_info} 可视化"

            # 发布图层
            success, actual_layer_name = publisher.create_layer(workspace_name, store_name, layer_name, title, tif_path)

            if success:
                # 添加到区域分组
                if region_name not in region_layers:
                    region_layers[region_name] = []

                region_layers[region_name].append(actual_layer_name)
        else:
            logger.warning(f"无法解析文件名: {file_name}，跳过此文件")
    # 发布原始TIF文件
    for tif_path in tqdm(raw_tifs, desc="发布原始TIF"):
        # 从文件路径解析区域和其他信息
        file_name = os.path.basename(tif_path)

        # 尝试提取区域名称、类型和日期
        parts = file_name.split('_')
        if len(parts) >= 3:
            region_name = parts[0]

            # 从路径中获取省份/城市分类
            if "province" in tif_path:
                region_type = "province"
            elif "city" in tif_path:
                region_type = "city"
            else:
                region_type = "unknown"

            # 确定日期或月份信息
            date_info = None
            for part in parts:
                if part.startswith("202"):  # 年份
                    date_info = part
                elif part.startswith("month"):
                    date_info = part

            if not date_info:
                date_info = "unknown_date"

            # 创建图层名称和标题
            store_name = f"{region_name}_{region_type}_raw_store"  # 使用不同的存储名
            layer_name = f"{region_name}_{date_info}_raw"  # 使用不同的图层名
            title = f"{region_name} {date_info} 原始数据"

            # 发布图层
            success, actual_layer_name = publisher.create_layer(workspace_name, store_name, layer_name, title, tif_path)

            if success:
                # 添加到区域分组 (可选)
                if f"{region_name}_raw" not in region_layers:
                    region_layers[f"{region_name}_raw"] = []

                region_layers[f"{region_name}_raw"].append(actual_layer_name)
        else:
            logger.warning(f"无法解析文件名: {file_name}，跳过此文件")    # 为每个区域创建图层组
    # 有条件地创建图层组
    if create_groups:
        for region_name, layers in tqdm(region_layers.items(), desc="创建图层组"):
            if len(layers) > 0:
                group_name = f"{region_name}_group"
                title = f"{region_name} 植被指数数据"
                publisher.create_layer_group(workspace_name, group_name, layers, title=title)

    logger.info("批量发布TIF文件完成！")


if __name__ == "__main__":
    # 配置信息
    GEOSERVER_URL = "http://localhost:8080/geoserver"  # 替换为您的GeoServer URL
    USERNAME = "admin"  # 替换为您的用户名
    PASSWORD = "geoserver"  # 替换为您的密码
    ROOT_DIR = r"D:\data\geoserver_tif"  # 替换为您的TIF文件目录
    WORKSPACE_NAME = "ndvi_data"  # 替换为您想使用的工作区名称

    # 批量发布
    batch_publish_tifs(GEOSERVER_URL, USERNAME, PASSWORD, ROOT_DIR, WORKSPACE_NAME)