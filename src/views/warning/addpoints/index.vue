<template>
  <div id="map">
    <WarningForm v-if="isShowWarningForm"></WarningForm>
    <ToolsSet></ToolsSet>
  </div>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref, watch, provide, inject } from 'vue';
import { initMap, map } from '@/views/home/initMap';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Feature from 'ol/Feature.js';
import Point from 'ol/geom/Point.js';
import WarningForm from '@/components/WarningForm/index.vue';
//@ts-ignore
import ToolsSet from '@/components/Tool/ToolsSet.vue';
import { addPointApi } from '@/api/test/index.ts';
let isShowWarningForm = ref(false);
provide(/* 注入名 */ 'isShowWarningForm', /* 值 */ isShowWarningForm);

let testpro = ref('testpro');
provide('testpro', testpro);
let lngForm = ref(116.397428);
let latForm = ref(39.90816);
// let warnrank = ref('');
let isWarningFormOpen = ref(true);
provide('lngForm', lngForm);
provide('latForm', latForm);
// provide('warnrank', warnrank);
provide('isWarningFormOpen', isWarningFormOpen);
onMounted(() => {
  initMap();
  // 创建一个矢量源来保存你的特性
  // var vectorSource = new VectorSource();

  // // 创建一个矢量图层用于显示矢量源上的特性
  // var vectorLayer = new VectorLayer({
  //   source: vectorSource,
  // });
  // watch(isShow, (newVal) => {
  //   if (newVal) {
  //     console.log('显示弹窗');
  //   } else {
  //     console.log('关闭弹窗');
  //   }
  // });
  // 将矢量图层添加到地图
  // map.value.addLayer(vectorLayer);
  // map.value.on('click', function (event) {
  //   console.log('触发click事件');
  //   isShow.value = true;
  //   // 创建新的点特性并添加到矢量源
  //   var pointFeature = new Feature({
  //     geometry: new Point(event.coordinate),
  //   });

  //   vectorSource.addFeature(pointFeature);
  // });
});
const handleCoordinateSelected = (coordinate: number[]) => {
  // console.log('坐标选择', coordinate);
};
// map.value.click(() => {
//   // 点击地图添加一个点
//   var coord = map.value.getEventCoordinate();
//   var feature = new Feature({
//     geometry: new Point(coord),
//   });
//   vectorSource.addFeature(feature);
// });
</script>
<style lang="scss" scoped>
#map {
  width: 100%;
  height: 100%;
}
</style>
