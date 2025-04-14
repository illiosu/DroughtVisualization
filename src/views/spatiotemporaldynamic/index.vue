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
  </div>
</template>

<script setup lang="ts">
import useLayOutSettingStore from '@/store/modules/setting';
import { ref, onMounted, computed } from 'vue';
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

let LatOutSettingStore = useLayOutSettingStore();

const year = ref('2000');

let chartWidth = ref('400px');
let chartHeight = ref('400px');
// 计算图片的路径
const imgSrc = computed(() => `/public/legend/${year.value}年土地利用图例.jpg`);
const timelineData = [
  {
    year: 2000,
  },
  {
    year: 2010,
  },
  {
    year: 2020,
  },
];

onMounted(() => {
  initMap();
  //默认加载2000年
  var geoserverLayer = new Tile({
    source: new TileWMS({
      //@ts-ignore
      ratio: 1,
      url: 'http://localhost:8080/geoserver/NanJing/wms',
      params: {
        LAYERS: 'NanJing:2000年地类重分类',
        STYLES: '',
        VERSION: '1.1.1',
        tiled: true,
      },
      serverType: 'geoserver',
    }),
  });
  // console.log('fff');
  map.value.addLayer(geoserverLayer);
  getBarOption('2000');
});

const handleActive = async (y: any) => {
  year.value = y;
  console.log(y);

  await getBarOption(y);
};
const getBarOption = async (y: any) => {
  let res_areas = await BarOptionApi(y);
  let areas = res_areas.data.map((item: any) => parseFloat(item.面积));
  let yAxis = res_areas.data.map((item: any) => item.土地类型);
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
