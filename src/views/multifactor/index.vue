<template>
  <div id="map" class="map_home" :class="{ fold: LatOutSettingStore.fold ? true : false }">
    <div class="mb-4">
      <el-button v-for="button in buttons" :key="button.text" :type="button.type" @click="handleButtonClick(button.text)">{{ button.text }}</el-button>
    </div>
  </div>
  <div class="left_chart">
    <span>区域筛选：</span>
    <el-select v-model="value" placeholder="Select" style="width: 240px">
      <el-option @click="handleSelectChange(item.value)" v-for="item in options" :key="item.value" :label="item.label" :value="item.value" :disabled="item.disabled" />
    </el-select>
    <el-row>
      <el-col :span="24"> <h1 style="font-weight: bold; text-align: center; font-size: larger">敏感性分析</h1></el-col>
    </el-row>
    <el-scrollbar height="200px" width="200px">
      {{ scrollView }}
    </el-scrollbar>
    <mychart :customOption="BarOption" :width="chartWidth" :height="chartHeight"></mychart>

    <!-- <mychart :customOption="dDBarOption1" :width="chartWidth" :height="chartHeight"></mychart> -->
  </div>
  <div class="right_chart">
    <mychart :customOption="PanelOption" :width="chartWidth" :height="chartHeight"></mychart>
    <mychart :customOption="PieOption" :width="chartWidth" :height="chartHeight"></mychart>
  </div>
</template>

<script setup lang="ts">
import useLayOutSettingStore from '@/store/modules/setting';
import { ref, onMounted } from 'vue';
import Tile from 'ol/layer/Tile';
import 'ol/ol.css';

//@ts-ignore
import mychart from '@/components/Echarts/template/index.vue';
import { PanelOption, BarOption, PieOption } from '@/components/Echarts/options/chartOption';
import TileWMS from 'ol/source/TileWMS.js';
import { initMap, map } from '@/views/home/initMap.ts';
import { clearMap, clearShpMap } from '@/views/home/clearMap';

import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import GeoJSON from 'ol/format/GeoJSON';
import { PieOptionApi, PanelOptionApi, BarOptionApi, ShpApi } from '@/api/test/index.ts';
let LatOutSettingStore = useLayOutSettingStore();

let chartWidth = ref('350px');
let chartHeight = ref('250px');

let selectedButton = ref('重分类坡向1');
const value = ref('');
let geoserverLayerName = ref('');
let scrollView = ref('');
onMounted(() => {
  initMap();

  handleButtonClick('综合评价结果');
});
const handleSelectChange = async (layerName: string) => {
  clearShpMap();
  let res = await ShpApi(layerName);
  let res2 = JSON.parse(res.data);
  let vectorSource = new VectorSource({
    features: new GeoJSON().readFeatures(res2),
  });
  let vectorLayer = new VectorLayer({
    source: vectorSource,
  });
  map.value.addLayer(vectorLayer);
};
const buttons = [
  { type: 'primary', text: '综合评价结果' },
  { type: 'primary', text: '重分类坡向' },
  { type: 'primary', text: '重分类坡度' },
  { type: 'primary', text: '重分类植被覆盖度' },
  { type: 'primary', text: '重分类水域缓冲区' },
  { type: 'primary', text: '重分类高程' },
  { type: 'primary', text: '重分类地类' },
] as const;
// const value = ref('');
const options = [
  // 玄武区,秦淮区,建邺区,鼓楼区,浦口区,栖霞区,雨花台区,江宁区,六合区,溧水区,高淳区
  {
    value: '玄武区',
    label: '玄武区',
  },
  {
    value: '秦淮区',
    label: '秦淮区',
    disabled: false,
  },
  {
    value: '建邺区',
    label: '建邺区',
  },
  {
    value: '鼓楼区',
    label: '鼓楼区',
  },
  {
    value: '浦口区',
    label: '浦口区',
  },
  {
    value: '栖霞区',
    label: '栖霞区',
  },
  {
    value: '雨花台区',
    label: '雨花台区',
  },
  {
    value: '江宁区',
    label: '江宁区',
  },
  {
    value: '六合区',
    label: '六合区',
  },
  {
    value: '溧水区',
    label: '溧水区',
  },
  {
    value: '高淳区',
    label: '高淳区',
  },
];
// function initMap() {
//   // 天地图地图层
//   var TianMapLayer = new Tile({
//     source: new XYZ({
//       url: 'http://t4.tianditu.com/DataServer?T=vec_w&tk=81e92153f7bd56bf35d42ceacbc4ab95&x={x}&y={y}&l={z}',
//     }),
//   });
//   var LandUse2020 = new Tile({
//     source: new TileWMS({
//       //瓦片的像素比例,不能设置为0，否则地图不展示。
//       //@ts-ignore
//       ratio: 1,
//       url: 'http://localhost:8080/geoserver/LE/wms',
//       params: {
//         LAYERS: 'LE:a2020地类重分类1',
//         STYLES: '',
//         VERSION: '1.1.1',
//         tiled: true,
//       },
//       serverType: 'geoserver',
//     }),
//   });
//   // 地图实例
//   map.value = new Map({
//     target: 'map', // 对应页面里 id 为 map 的元素
//     layers: [TianMapLayer, LandUse2020],
//     // 地图视图
//     view: new View({
//       projection: 'EPSG:4326', // 坐标系，有EPSG:4326和EPSG:3857
//       // 自定义
//       //地图中心点
//       center: [118.81, 31.99],
//       //   minZoom: 10, // 地图缩放最小级别
//       zoom: 9, // 地图缩放级别（打开页面时默认级别）
//     }),
//   });
// }
const handleButtonClick = async (layerName: string) => {
  selectedButton.value = layerName;
  geoserverLayerName.value = layerName;
  clearMap();
  // geoserver图层
  // var geoserverLayer = new Tile({
  //   source: new TileWMS({
  //     //@ts-ignore
  //     ratio: 1,
  //     url: 'http://localhost:8080/geoserver/NanJing/wms',
  //     params: {
  //       LAYERS: layerName,
  //       STYLES: '',
  //       VERSION: '1.1.1',
  //       tiled: true,
  //     },
  //     serverType: 'geoserver',
  //   }),
  // });
  // map.value.addLayer(geoserverLayer);
  addGeoserverLayer(layerName);

  //面积
  let res_areas = await BarOptionApi(layerName);
  let areas = res_areas.data.map((item: any) => parseFloat(item.面积));
  BarOption.series[0].data = areas;

  //面积占比
  let res_areaProportion = await PieOptionApi(layerName);
  const areaProportion = res_areaProportion.data.map((item: any) => {
    return {
      value: parseFloat(item['面积占比']),
      name: item.敏感度,
    };
  });
  PieOption.series[0].data = areaProportion;

  // 平均等级
  let res_aveRank = await PanelOptionApi(layerName);
  PanelOption.series[0].data[0].value = res_aveRank.data[0]['平均等级'];
  if (layerName == '综合评价结果') {
    console.log(1111111);
    scrollView.value =
      '综合各生态因子的敏感性分析图，在GIS中进行加权叠加分析后，得出南京市生态敏感性综合分析图，其生态敏感性多集中在低敏感和中度敏感内，南京市整体生态敏感性偏中低等，极度敏感性分区占总面积的8.33%，高度敏感占比 17.10%,中度敏感占比30.84%，不敏感区和低敏感区占比14.82%和28.92%。因此，南京市综合生态敏感性处于中度敏感。';
  }
  if (layerName == '重分类坡向') {
    scrollView.value =
      '光照强度和日照长度随坡向变化，植被生长受叶片辐射的影响。极度敏感区大多分布在南京市内长江、秦淮河等河流水体,占南京市总面积的16.41%，高度敏感区、中度敏感区和低敏感区则在南京市内部较为均匀的分布且占比极高，分别为21.89%、24.38%和22.39%。不敏感区占14.93%，主要分布在南京市丘陵地带，采光良好。综上所述平潭岛的坡向整体敏感性处于中度敏感。';
  }
  if (layerName == '重分类坡度') {
    scrollView.value =
      '一般来说，坡度的大小与水士流失所发生的概率呈正相关，坡度也是影响地表物质流动和承载能力的主要因素，制约着生产力的空间分布。坡度愈大敏感性愈高。研究区域的地貌类型为丘陵和平原，包括低山、丘陵、岗地、平原、洼地、河流和湖泊等多种地貌单元，总体上来看地形起伏较小。平潭岛坡度变化较大的区域在南京市中部地区，即中度，高度，极度敏感地区占比极少，分别仅为5.18%、1.98%和1.85%；而不敏感区和低敏感区均匀分布在平南京市内部，坡度主要在10°以下。由上述可知，平潭的坡度生态敏感性多处于不敏感。';
  }
  if (layerName == '重分类植被覆盖度') {
    scrollView.value =
      '生态敏感性越低的区域其生态稳定性就越高，生态稳定性愈高的区域群落结构愈丰富，植被覆盖度愈高。植被覆盖度在系统中使用归一化植被指数 NDVI反映。南京市NDVI的生态敏感性普遍较高，大部分地区为高敏感地区。其中，高敏感区和极度敏感区分别占34.13%和30.21%。而南京市较少地区为不敏感和轻度敏感地区，占比为6.08%和10.55%，甚至不到五分之一，主要分布在研究区水域附近。南京市为新一线城市，现代化建设发达，但植被覆盖度较差，形式严峻，需要着重注意城市的绿色发展。因此，南京市的植被覆盖度生态敏感性处于极度敏感。';
  }
  if (layerName == '重分类水域缓冲区') {
    scrollView.value =
      '水体在生态环境的调节和净化方面有着关键性的影响，对生态系统起关键作用，且建筑的布局受水域分布的影响。本次研究根据南京市地质条件，选取研究区内的主要水域作为生态敏感性的评价因子，以与水体本身的距离确定缓冲半径，即设立100m以内、500m、1000m和3000m以外的缓冲距离，其对应的敏感性等级分区从低到高确定为极度敏感区、重度敏感区、中度敏感区、轻度敏感区和不敏感区。由图可以看出，不敏感区占比最大，为南京市总面积的70.47%，中度敏感区和高度敏感区占研究区总面积的4.62%和3.81%，低敏感占总面积的12.77%，极度敏感区占总面积的8.33%。因此，南京市水域缓冲区敏感性为不敏感。';
  }
  if (layerName == '重分类高程') {
    scrollView.value =
      '南京市四周地势低平，位于宁镇扬丘陵地区，地形以低山、缓岗为主，而温度、大气等自然环境因素受高程的影响，从而进一步影响地表各类植物的生长与分布状况，高程愈大，生态敏感性愈强。极度敏感和高度敏感区占比不到4%，两者高程在100m以上，分布位置大致在南京市中部地区，在南京市东南部偶有分布。中度敏感区占比为6.68%，分布位置与极高度敏感区大体一致。低敏感区和不敏感区分别占总面积的32.58%和57.52%，占据南京市的大部分。因此，平潭的高程敏感性多处于不敏感。';
  }
  if (layerName == '重分类地类') {
    scrollView.value =
      '南京市士地利用类型主要分为耕地、林地、草地、水域，建设用地和未利用土地六种类型。南京市土地利用的生态敏感性普遍较高，权重为0.335。由图可知中度敏感区占总面积的51.65%，已经超过南京市总面积的一半，是面积最大的部分，以耕地和建设用地为主；极度敏感和高度敏感占比较低，分别占南京市总面积的11.34%和10.11%，从空间分布上看，二者主要集中在水域和林地，需要加强保护措施；而不敏感区分布在南京市的城中区，如鼓楼区、栖霞区、秦淮区、建邺区东部以及江宁区北部，占总用地的 26.54%。因此，南京市的士地利用类型的生态敏感性处于中度敏感。';
  }
};
const addGeoserverLayer = (layerName: string) => {
  console.log('addGeoserverLayer');
  // geoserver图层
  var geoserverLayer = new Tile({
    source: new TileWMS({
      //@ts-ignore
      ratio: 1,
      url: 'http://localhost:8080/geoserver/NanJing/wms',
      params: {
        LAYERS: layerName,
        STYLES: '',
        VERSION: '1.1.1',
        tiled: true,
      },
      serverType: 'geoserver',
    }),
  });
  map.value.addLayer(geoserverLayer);
};
</script>

<style lang="scss" scoped>
.map_home {
  width: calc(100vw - $base-slider-width);
  height: calc(100vh - $base-tabbar-heigth);
  padding: 0;
  margin: 0;
  &.fold {
    width: calc(100vw - $base-slider-min-width);
    left: $base-slider-min-width;
  }
  .mb-4 {
    position: absolute;
    z-index: 999;
    left: 10vw;
    right: 10vw;
    top: 10px;
  }
  //   width: 100%;
  //   height: 100%;
  //   border: 1px solid #eee;
}
.left_chart {
  position: absolute;
  left: 100px;
  top: 80px;
  width: 350px;
  background-color: whitesmoke;
  background-color: rgba(0, 0, 0, 0.3);
  border-radius: 15px;
}
.right_chart {
  position: absolute;
  right: 100px;
  top: 80px;
  background-color: whitesmoke;
  background-color: rgba(0, 0, 0, 0.3);
  border-radius: 15px;
  // width: 100%;
  // height: 100%;
}
.scrollbar-demo-item {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 50px;
  margin: 10px;
  text-align: center;
  border-radius: 4px;
  background: var(--el-color-primary-light-9);
  color: var(--el-color-primary);
}
</style>
