<template>
  <div class="tools-bar" v-show="isShowToolbar">
    <!-- <div class="tool-item border" @click="activeTool('measurePolyline')">
      <img :src="icon1" alt="" />
      <div class="item-text">测距</div>
    </div>
    <div class="tool-item border" @click="activeTool('measurePolygon')">
      <img :src="icon2" alt="" />
      <div class="item-text">测面</div>
    </div>
    <div class="tool-item border" @click="activeTool('getLocation')">
      <img :src="icon3" alt="" />
      <div class="item-text">定位</div>
    </div> -->
    <div class="tool-item border" @click="activeTool('MarkerTool')">
      <img :src="icon4" alt="" />
      <div class="item-text">标记</div>
    </div>
    <!-- <div class="tool-item border" @click="activeTool('latlngGraticule')">
      <img :src="icon5" alt="" />
      <div class="item-text">经纬网</div>
    </div> -->
    <div class="tool-item border" @click="activeTool('clearAll')">
      <img :src="icon6" alt="" />
      <div class="item-text">清除</div>
    </div>
    <div class="tool-item" @click="isShowToolbar = !isShowToolbar">
      <img :src="icon7" alt="" class="img" />
      <div class="item-text">隐藏</div>
    </div>
  </div>
  <div class="tool-bar-tip" v-show="!isShowToolbar">
    <img :src="icon8" alt="" class="img tip" title="工具栏" @click="isShowToolbar = !isShowToolbar" />
  </div>
  <!-- <Local v-show="localStatus" @closeLocal="closeLocal" @locationPoint="locationPoint"></Local> -->
  <!-- <MarkTool v-if="isShouMark" v-show="isHideMark" @hideMarkPanel="hideMarkPanel"></MarkTool> -->
  <div id="mouseTipClass">请选择一个敏感点</div>
</template>

<script setup>
// import { ref } from 'vue';
import { onMounted, reactive, ref, watch, provide, inject, watchEffect } from 'vue';

import icon1 from '@/assets/tools/测距.png';
import icon2 from '@/assets/tools/测面积.png';
import icon3 from '@/assets/tools/定位.png';
import icon4 from '@/assets/tools/标记.png';
import icon5 from '@/assets/tools/经纬网.png';
import icon6 from '@/assets/tools/删除.png';
import icon7 from '@/assets/tools/隐藏.png';
import icon8 from '@/assets/tools/工具.png';
// import { mapObj } from "../mapInfoObj/initMap";
// import Config from "../mapInfoObj/mapConfig";
// import * as turf from "@turf/turf";
// import Local from './Local.vue'
// import MarkTool from './MarkTool.vue';
import Overlay from 'ol/Overlay.js';
import { initMap, map } from '@/views/home/initMap';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Feature from 'ol/Feature.js';
import Point from 'ol/geom/Point.js';
// import eventBus from '@/utils/eventBus.ts';
// import mitt from 'mitt';
// import emitter from '@/utils/mitt.ts';
import { Style, Icon } from 'ol/style';

const isShouMark = ref(false);
const isHideMark = ref(false);
const localStatus = ref(false);
const isShowToolbar = ref(false);
// const vectorSource = new VectorSource();
const isShowWarningForm = inject('isShowWarningForm');
const lngForm = inject('lngForm');
const latForm = inject('latForm');
let popup = null;
// const warnrank = inject('warnrank');
// const isWarningFormOpen = inject('isWarningFormOpen');
// console.log(isWarningFormOpen.value);
// console.log(lngForm.value);
// console.log(isShowWarningForm.value);

// let lng = ref(444);
// provide('lngg', lng);

// let followShow = ref(false);
// 测距的方法
// const measureDis = () => {
//   mapObj.map.pm.enableDraw('Line', {
//     snappable: true,
//     snapDistance: 20,
//     finishOn: 'dblclick',
//     allowSelfIntersection: false,
//     tooltips: true,
//     templineStyle: {
//       color: 'red',
//       dashArray: '5, 5',
//     },
//     hintlineStyle: {
//       color: 'red',
//       dashArray: '5, 5',
//     },
//     pathOptions: {
//       color: 'red',
//       fillColor: 'red',
//       fillOpacity: 0.4,
//       opacity: 1,
//       weight: 2,
//       clickable: true,
//     },
//   })
//   // 添加到Config.ToolLayers中
//   mapObj.map.on('pm:create', (e) => {
//     Config.ToolLayers.addLayer(e.layer)
//     const lineGeoJSON = e.layer.toGeoJSON();
//     const length = turf.length(lineGeoJSON);
//     e.layer.bindPopup(`距离为：${(length / 1000).toFixed(2)}公里`).openPopup();
//   })
// }
// 测面的方法
// const measureArea = () => {

//   mapObj.map.pm.enableDraw('Polygon', {
//     snappable: true,
//     snapDistance: 20,
//     finishOn: 'dblclick',
//     allowSelfIntersection: false,
//     tooltips: true,
//     templineStyle: {
//       color: 'red',
//       dashArray: '5, 5',
//     },
//     hintlineStyle: {
//       color: 'red',
//       dashArray: '5, 5',
//     },
//     pathOptions: {
//       color: 'red',
//       fillColor: 'red',
//       fillOpacity: 0.4,
//       opacity: 1,
//       weight: 2,
//       clickable: true,
//     },
//   })
//   // 添加到Config.ToolLayers中
//   mapObj.map.on('pm:create', (e) => {
//     Config.ToolLayers.addLayer(e.layer)
//     const polygonGeoJSON = e.layer.toGeoJSON();
//     const area = turf.area(polygonGeoJSON);
//     e.layer.bindPopup(`面积为：${(area / 1000000).toFixed(2)}平方千米`).openPopup();

//   })
// }
//
// 标记方法
const markPoint = () => {
  popup.style.display = 'block';
  window.addEventListener('mousemove', moveHandler);
  map.value.on('click', handleMapClick);
};
// 经纬网绘制方法
let latlngLayer = null;
onMounted(() => {
  console.log('ToolsSet onMounted');
  popup = document.getElementById('mouseTipClass');
  // console.log(follow);
});

//设置popup位置
const moveHandler = (event) => {
  var x = event.clientX;
  var y = event.clientY;
  popup.style.left = x + 20 + 'px';
  popup.style.top = y + -20 + 'px';
};
// 鼠标点击事件处理器
const handleMapClick = (event) => {
  // console.log('触发click事件');
  isShowWarningForm.value = true;
  // isWarningFormOpen.value = true;

  // var vectorLayer = new VectorLayer({
  //   source: vectorSource,
  // });

  // var pointFeature = new Feature({
  //   geometry: new Point(event.coordinate),
  // });
  // // console.log(warnrank.value);
  // let iconUrl = '';
  // switch (warnrank.value) {
  //   case '普通预警':
  //     iconUrl = '/public/tools/标记.png';
  //     break;
  //   case '高级预警':
  //     iconUrl = '/public/tools/工具.png';
  //     break;
  //   default:
  //     iconUrl = '/public/tools/标记.png';
  // }
  // pointFeature.setStyle(
  //   new Style({
  //     image: new Icon({
  //       src: iconUrl,
  //       scale: 0.5, // 调整图标的大小
  //     }),
  //   }),
  // );
  // vectorSource.addFeature(pointFeature);
  // map.value.addLayer(vectorLayer);

  if (isShowWarningForm.value) {
    popup.style.display = 'none';
    // 使用保存的处理器移除事件监听
    window.removeEventListener('mousemove', moveHandler);
    map.value.un('click', handleMapClick);
  }
  // instance.proxy.$emit('coor-selected', { lng: event.coordinate[0], lat: event.coordinate[1] });
  // emitter.on('foo', (e) => console.log()); //emitter
  // eventBus.emit('coor-selected', { lng: event.coordinate[0], lat: event.coordinate[1] });
  lngForm.value = event.coordinate[0];
  latForm.value = event.coordinate[1];
};
const activeTool = (type) => {
  // 测距
  // if (type == 'measurePolyline') {
  //   measureDis();
  //   return;
  // }
  // // 测面积
  // if (type == 'measurePolygon') {
  //   measureArea();
  //   return;
  // }
  // 定义一个状态标识是否已经点击过
  // let clickedOnce = false;

  // 标记工具
  if (type == 'MarkerTool') {
    markPoint();

    return;
  }

  // 定位
  if (type == 'getLocation') {
    localStatus.value = true;
    return;
  }
  // 经纬网
  if (type == 'latlngGraticule') {
    return;
  }
  // 清除工具
  if (type == 'clearAll') {
    // clear();
    popup.style.display = 'none';
    // 使用保存的处理器移除事件监听
    window.removeEventListener('mousemove', moveHandler);
    map.value.un('click', handleMapClick);

    return;
  }
};

const hideMarkPanel = () => {
  isHideMark.value = false;
};
</script>

<style scoped>
.tool-bar-tip {
  position: absolute;
  right: 1px;
  top: 15px;
  width: 40px;
  height: 40px;
  background: #ffffff;
  box-shadow: 0px 2px 6px 0px #c5c5c5;
  border-radius: 4px;
  display: flex;
  align-items: center;
  z-index: 450;
}

.tools-bar {
  position: absolute;
  right: 20px;
  top: 15px;
  width: 260px;
  height: 40px;
  background: #ffffff;
  box-shadow: 0px 2px 6px 0px #c5c5c5;
  border-radius: 4px;
  display: flex;
  align-items: center;
  z-index: 450;
}

.tool-item {
  height: 22px;
  width: 80px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  user-select: none;
}

.border {
  border-right: 1px solid #dbdee2;
}

.item-text {
  font-size: 16px;
  font-family: Alibaba PuHuiTi;
  font-weight: 400;
  color: #475366;
  line-height: 22px;
}

.img {
  width: 22px;
  height: 22px;
}

.tip {
  margin: 0 9px;
  cursor: pointer;
}
#mouseTipClass {
  display: none;
  position: fixed;
  z-index: 9999;
  background: white;
  border: 1px solid gray;
  padding: 4px;
  border-radius: 4px;
}
</style>
