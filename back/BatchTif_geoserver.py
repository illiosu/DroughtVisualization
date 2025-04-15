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

        # 添加日志以诊断问题
        logger.info(f"文件路径: {tif_path}")
        logger.info(f"提取的文件名: {file_basename}")
    
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
                return False, None

        except Exception as e:
            logger.error(f"发布图层 {layer_name} 时出错: {str(e)}")
            return False, None

    def clean_workspace(self, workspace):
        """删除工作区中的所有图层和存储"""
        logger.info(f"开始清理工作区 {workspace} 中的内容...")
        
        # 获取工作区中的所有图层
        response = requests.get(
            f"{self.geoserver_url}/rest/layers.json",
            auth=self.auth
        )
        
        if response.status_code == 200:
            layers_data = response.json()
            if 'layers' in layers_data and 'layer' in layers_data['layers']:
                layers = layers_data['layers']['layer']
                for layer in layers:
                    layer_name = layer['name']
                    if layer_name.startswith(f"{workspace}:"):
                        # 删除图层
                        logger.info(f"删除图层 {layer_name}")
                        requests.delete(
                            f"{self.geoserver_url}/rest/layers/{layer_name}",
                            auth=self.auth
                        )
        
        # 获取并删除所有coverage stores
        response = requests.get(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores.json",
            auth=self.auth
        )
        
        if response.status_code == 200:
            stores_data = response.json()
            if 'coverageStores' in stores_data and 'coverageStore' in stores_data['coverageStores']:
                stores = stores_data['coverageStores']['coverageStore']
                for store in stores:
                    store_name = store['name']
                    logger.info(f"删除存储 {store_name}")
                    # 递归删除存储及关联的所有资源
                    requests.delete(
                        f"{self.geoserver_url}/rest/workspaces/{workspace}/coveragestores/{store_name}?recurse=true",
                        auth=self.auth
                    )
        
        # 获取并删除所有图层组
        response = requests.get(
            f"{self.geoserver_url}/rest/workspaces/{workspace}/layergroups.json",
            auth=self.auth
        )
        
        if response.status_code == 200:
            groups_data = response.json()
            if 'layerGroups' in groups_data and 'layerGroup' in groups_data['layerGroups']:
                groups = groups_data['layerGroups']['layerGroup']
                for group in groups:
                    group_name = group['name']
                    logger.info(f"删除图层组 {group_name}")
                    requests.delete(
                        f"{self.geoserver_url}/rest/workspaces/{workspace}/layergroups/{group_name}",
                        auth=self.auth
                    )
        
        logger.info(f"工作区 {workspace} 清理完成")
        return True


def detect_data_type(file_name):
    """检测数据类型（MOD11A2-温度或MOD13A3-NDVI）"""
    if "_Tep_" in file_name:
        return "LST"
    elif "_NDVI_" in file_name:
        return "NDVI"
    else:
        return "UNKNOWN"


def batch_publish_tifs(geoserver_url, username, password, root_dir, workspace_name="remote_sensing", clean_first=True):
    """批量发布TIF文件"""
    publisher = GeoServerPublisher(geoserver_url, username, password)

    # 创建工作区
    if not publisher.create_workspace(workspace_name):
        return
    
    # 清理之前数据
    if clean_first:
        publisher.clean_workspace(workspace_name)
    
    # 找到所有TIF文件，并按数据类型分类
    lst_vis_tifs = []
    lst_raw_tifs = []
    ndvi_vis_tifs = []
    ndvi_raw_tifs = []
    unknown_tifs = []

    # 递归查找所有TIF文件
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if not file.endswith(".tif"):
                continue
                
            full_path = os.path.join(root, file)
            data_type = detect_data_type(file)
            
            if data_type == "LST":
                if "_vis" in file:
                    lst_vis_tifs.append(full_path)
                else:
                    lst_raw_tifs.append(full_path)
            elif data_type == "NDVI":
                if "_vis" in file:
                    ndvi_vis_tifs.append(full_path)
                else:
                    ndvi_raw_tifs.append(full_path)
            else:
                unknown_tifs.append(full_path)

    logger.info(f"找到 LST可视化TIF: {len(lst_vis_tifs)}个, LST原始TIF: {len(lst_raw_tifs)}个")
    logger.info(f"找到 NDVI可视化TIF: {len(ndvi_vis_tifs)}个, NDVI原始TIF: {len(ndvi_raw_tifs)}个")
    if unknown_tifs:
        logger.warning(f"找到 {len(unknown_tifs)} 个无法识别数据类型的TIF文件")

    # 按数据类型组织图层
    region_layers = {}

    # 1. 处理LST可视化TIF文件
    for tif_path in tqdm(lst_vis_tifs, desc="发布LST可视化TIF"):
        process_tif_file(publisher, tif_path, workspace_name, "LST", "vis", region_layers)

    # 2. 处理LST原始TIF文件
    for tif_path in tqdm(lst_raw_tifs, desc="发布LST原始TIF"):
        process_tif_file(publisher, tif_path, workspace_name, "LST", "raw", region_layers)

    # 3. 处理NDVI可视化TIF文件
    for tif_path in tqdm(ndvi_vis_tifs, desc="发布NDVI可视化TIF"):
        process_tif_file(publisher, tif_path, workspace_name, "NDVI", "vis", region_layers)

    # 4. 处理NDVI原始TIF文件
    for tif_path in tqdm(ndvi_raw_tifs, desc="发布NDVI原始TIF"):
        process_tif_file(publisher, tif_path, workspace_name, "NDVI", "raw", region_layers)

    logger.info("批量发布TIF文件完成！")


def process_tif_file(publisher, tif_path, workspace_name, data_type, vis_type, region_layers):
    """处理单个TIF文件并发布为图层"""
    file_name = os.path.basename(tif_path)
    
    # 尝试提取区域名称、类型和日期
    parts = file_name.split('_')
    if len(parts) < 3:
        logger.warning(f"无法解析文件名: {file_name}，跳过此文件")
        return
        
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
        if part.startswith("202"):  # 年份日期格式：20240101
            date_info = part
            break
        elif part.startswith("month"):  # 月份格式：month1
            date_info = part
            break
    
    if not date_info:
        date_info = "unknown_date"
    
    # 创建带有数据类型和日期标识的唯一存储名
    store_name = f"{region_name}_{region_type}_{data_type}_{date_info}_{vis_type}_store"
    layer_name = f"{data_type}_{region_name}_{date_info}_{vis_type}"
        
    if data_type == "LST":
        title = f"地表温度 {region_name} {date_info} {'可视化' if vis_type == 'vis' else '原始数据'}"
    else:  # NDVI
        title = f"植被指数 {region_name} {date_info} {'可视化' if vis_type == 'vis' else '原始数据'}"
    
    # 发布图层
    success, actual_layer_name = publisher.create_layer(workspace_name, store_name, layer_name, title, tif_path)
    
    if success:
        # 添加到区域分组
        group_key = f"{data_type}_{region_name}_{vis_type}"
        if group_key not in region_layers:
            region_layers[group_key] = []
        
        region_layers[group_key].append(actual_layer_name)


if __name__ == "__main__":
    # 配置信息
    GEOSERVER_URL = "http://localhost:8080/geoserver"  # 替换为您的GeoServer URL
    USERNAME = "admin"  # 替换为您的用户名
    PASSWORD = "geoserver"  # 替换为您的密码
    ROOT_DIR = r"D:\data\geoserver_tif"  # 替换为您的TIF文件目录
    WORKSPACE_NAME = "remote_sensing"  # 替换为您想使用的工作区名称

    # 批量发布
    batch_publish_tifs(GEOSERVER_URL, USERNAME, PASSWORD, ROOT_DIR, WORKSPACE_NAME, clean_first=True)