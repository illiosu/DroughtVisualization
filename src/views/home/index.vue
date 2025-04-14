<template>
  <div id="map" class="map_home" :class="{ fold: LatOutSettingStore.fold ? true : false }">
    <measureTool :map="map"></measureTool>
    <div class="checkbox_map">
      <el-checkbox style="display: block" :true-value="checked" :false-value="uncheckd" label="南京市影像图" @change="handleCheckbox1Change" size="large" />
      <el-checkbox style="display: block" :true-value="checked" :false-value="uncheckd" label="南京市县区边界" @change="handleCheckbox2Change" size="large" />
    </div>
  </div>
</template>

<script setup lang="ts">
import useLayOutSettingStore from '@/store/modules/setting';
import { ref, onMounted, defineExpose } from 'vue'; // vue相关方法
import { Map, View } from 'ol'; // 地图实例方法、视图方法
import Tile from 'ol/layer/Tile'; // 瓦片渲染方法
// import OSM from 'ol/source/OSM'; // OSM瓦片【OSM不能在实际开发中使用，因为OSM里中国地图的边界有点问题！！！！】
import 'ol/ol.css'; // 地图样式
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import GeoJSON from 'ol/format/GeoJSON';
import * as source from 'ol/source';
import XYZ from 'ol/source/XYZ.js';
// import ecoassess from '@/components/Echarts/ecoassess/index.vue';
//@ts-ignore
import mychart from '@/components/Echarts/template/index.vue';
import { initMap } from '@/views/home/initMap.ts';
import TileWMS from 'ol/source/TileWMS.js';
//@ts-ignore
import measureTool from '@/components/MeasureTool/index.vue';
import { map } from '@/views/home/initMap.ts';
import { clearMap, removeSpecificLayer } from './clearMap';
let LatOutSettingStore = useLayOutSettingStore();
import { test } from '@/api/test/index.ts';
let checked = ref(true);
let uncheckd = ref(false);
onMounted(() => {
  initMap();
});
const handleCheckbox1Change = (e: any) => {
  // console.log(e);
  if (e) {
    addGeoserverLayer('南京市影像图');
  } else {
    // clearMap();
    removeSpecificLayer('南京市影像图');
  }
};
const handleCheckbox2Change = (e: any) => {
  // console.log(e);
  if (e) {
    addGeoserverLayer('njs');
  } else {
    // clearMap();
    removeSpecificLayer('njs');
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
  geoserverLayer.set('name', layerName);
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
  .checkbox_map {
    position: absolute;
    z-index: 100;
    right: 3vw;
    top: 18vh;
  }
  //   width: 100%;
  //   height: 100%;
  //   border: 1px solid #eee;
}
</style>
