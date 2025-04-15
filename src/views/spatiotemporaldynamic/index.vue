<template>
  <div id="map" class="map_home" :class="{ fold: LatOutSettingStore.fold ? true : false }">
    <div class="timeline-wraper">
      <div class="timeline">
        <Timeline :timelineData="timelineData" @update:active="handleActive"></Timeline>
      </div>
    </div>
    <div class="chart-wraper">
      <mychart :customOption="LandTypeOption" :width="chartWidth" :height="chartHeight"></mychart>
    </div>
    <div class="legend-wraper">
      <img :src="imgSrc" alt="" />
    </div>
    <div class="radio-wraper" style="position: absolute; top: 10vh; left: 5vw; z-index: 100">
      <el-radio-group v-model="dataType" @change="updateLayer">
        <el-radio value="LST" size="large">LST</el-radio>
        <el-radio value="NDVI" size="large">NDVI</el-radio>
      </el-radio-group>
    </div>
    <div class="dropdown-wraper" style="position: absolute; top: 5vh; left: 5vw; z-index: 100">
      <el-dropdown @command="handleCityChange">
        <span class="el-dropdown-link">
          {{ selectedCity }}
          <el-icon class="el-icon--right">
            <arrow-down />
          </el-icon>
        </span>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="fuzhoushi">福州市</el-dropdown-item>
            <el-dropdown-item command="longyanshi">龙岩市</el-dropdown-item>
            <el-dropdown-item command="nanningshi">南宁市</el-dropdown-item>
            <el-dropdown-item command="ningdeshi">宁德市</el-dropdown-item>
            <el-dropdown-item command="putianshi">莆田市</el-dropdown-item>
            <el-dropdown-item command="quanzhoushi">泉州市</el-dropdown-item>
            <el-dropdown-item command="sanmingshi">三明市</el-dropdown-item>
            <el-dropdown-item command="xiamenshi">厦门市</el-dropdown-item>
            <el-dropdown-item command="zhangzhoushi">漳州市</el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
  </div>
</template>

<script setup lang="ts">
import useLayOutSettingStore from '@/store/modules/setting';
import { ref, onMounted, computed, watch } from 'vue';
// import { Map, View } from 'ol';
import Tile from 'ol/layer/Tile';
import 'ol/ol.css'; // 地图样式

//@ts-ignore
import mychart from '@/components/Echarts/template/index.vue';

import TileWMS from 'ol/source/TileWMS.js';
//@ts-ignore
import Timeline from '@/components/TimeLine/index.vue';
import { initMap } from '@/views/home/initMap.ts';
import { map } from '@/views/home/initMap.ts';
import { LandTypeOption } from '@/components/Echarts/options/chartOption';
import { BarOptionApi } from '@/api/test/index.ts';

// 修改变量名称以更准确地反映其用途
const dataType = ref('NDVI');
const radio2 = ref('1');
const radio3 = ref('1');
let LatOutSettingStore = useLayOutSettingStore();

// 添加城市选择状态
const selectedCity = ref('宁德市');
const selectedCityCode = ref('ningdeshi');

// 记录当前选择的月份
const currentMonth = ref(1);
const monthNames = ['一月份', '二月份', '三月份'];

let chartWidth = ref('400px');
let chartHeight = ref('400px');
// 计算图片的路径
const imgSrc = computed(() => `/public/legend/${year.value}年土地利用图例.jpg`);
const timelineData = [{ year: '一月份' }, { year: '二月份' }, { year: '三月份' }];

// 当前加载的图层
let currentLayer = ref(null);
const year = ref('2000');

// 处理城市变更
const handleCityChange = (city) => {
  selectedCityCode.value = city;
  // 设置显示名称
  const cityMap = {
    fuzhoushi: '福州市',
    longyanshi: '龙岩市',
    nanningshi: '南宁市',
    ningdeshi: '宁德市',
    putianshi: '莆田市',
    quanzhoushi: '泉州市',
    sanmingshi: '三明市',
    xiamenshi: '厦门市',
    zhangzhoushi: '漳州市',
  };
  selectedCity.value = cityMap[city];
  updateLayer();
};

// 根据选择的城市、数据类型和月份构建图层名称
const getLayerName = () => {
  // 构建月份字符串，格式为YYYYMMDD
  const monthStr = currentMonth.value.toString().padStart(2, '0');

  if (dataType.value === 'NDVI') {
    return `remote_sensing:NDVI_${selectedCityCode.value}_2024${monthStr}01.tif_raw`;
  } else {
    // LST
    return `remote_sensing:LST_${selectedCityCode.value}_month${currentMonth.value}_vis`;
  }
};

// 更新地图图层
const updateLayer = () => {
  // 如果已有图层，先移除
  if (currentLayer.value) {
    map.value.removeLayer(currentLayer.value);
  }

  // 创建新图层
  const layerName = getLayerName();
  console.log('加载图层:', layerName);

  const geoserverLayer = new Tile({
    source: new TileWMS({
      //@ts-ignore
      ratio: 1,
      url: 'http://localhost:8080/geoserver/remote_sensing/wms',
      params: {
        LAYERS: layerName,
        STYLES: '',
        VERSION: '1.1.1',
        tiled: true,
      },
      serverType: 'geoserver',
    }),
  });

  // 添加新图层并保存引用
  map.value.addLayer(geoserverLayer);
  currentLayer.value = geoserverLayer;
};

onMounted(() => {
  initMap();
  // 初始加载默认图层
  updateLayer();
  getBarOption('2000');
});

// 处理时间轴的月份变化
const handleActive = async (y) => {
  year.value = y;
  console.log('选择的月份:', y);

  // 根据月份字符串获取月份索引
  const monthIndex = monthNames.findIndex((month) => month === y) + 1;
  currentMonth.value = monthIndex;

  // 更新图层
  updateLayer();

  await getBarOption(y);
};

const getBarOption = async (y) => {
  let res_areas = await BarOptionApi(y);
  let areas = res_areas.data.map((item) => parseFloat(item.面积));
  let yAxis = res_areas.data.map((item) => item.土地类型);
  LandTypeOption.title.text = y + '年土地类型面积(km²)';
  LandTypeOption.yAxis.data = yAxis;
  LandTypeOption.series[0].data = areas;
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
  .legend-wraper {
    position: absolute;
    top: 70%;
    right: calc($legend-left);
    z-index: 100;
    img {
      // margin: 10px;
      width: 150px;
    }
  }
  .timeline-wraper {
    // float: right;
    position: absolute;
    z-index: 100;
    right: 5vw;
    top: 10vh;
  }
  .chart-wraper {
    position: absolute;
    left: 2vw;
    top: 35%;
    z-index: 100;
  }

  //   width: 100%;
  //   height: 100%;
  //   border: 1px solid #eee;
}

// .left_chart {
//   position: absolute;
//   left: 100px;
//   top: 50px;
//   background-color: whitesmoke;
//   background-color: rgba(0, 0, 0, 0.3);
//   border-radius: 15px;
// }
// .right_chart {
//   position: absolute;
//   right: 100px;
//   top: 50px;
//   background-color: whitesmoke;
//   background-color: rgba(0, 0, 0, 0.3);
//   border-radius: 15px;
//   // width: 100%;
//   // height: 100%;
// }
</style>
