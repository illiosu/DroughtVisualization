import { ref, onMounted, provide } from 'vue'; // vue相关方法
import { Map, View } from 'ol'; // 地图实例方法、视图方法
import Tile from 'ol/layer/Tile'; // 瓦片渲染方法
// import OSM from 'ol/source/OSM'; // OSM瓦片【OSM不能在实际开发中使用，因为OSM里中国地图的边界有点问题！！！！】
import 'ol/ol.css'; // 地图样式
import * as source from 'ol/source';
import XYZ from 'ol/source/XYZ.js';
// import ecoassess from '@/components/Echarts/ecoassess/index.vue';
import TileWMS from 'ol/source/TileWMS.js';
import Control from 'ol/control/Control.js';
import { defaults as defaultControls, FullScreen, ScaleLine } from 'ol/control';
import { clearMap } from './clearMap';
const map = ref(); // 存放地图实例
function initMap() {
  console.log('initMap');

  // 天地图地图层
  var TianMapLayer = new Tile({
    source: new XYZ({
      url: 'http://t4.tianditu.com/DataServer?T=vec_w&tk=81e92153f7bd56bf35d42ceacbc4ab95&x={x}&y={y}&l={z}',
    }),
  });

  // Open Street Map 地图层
  //@ts-ignore
  var openStreetMapLayer = new Tile({
    source: new source.OSM(),
  });

  // 地图实例
  map.value = new Map({
    target: 'map', // 对应页面里 id 为 map 的元素
    // 设置地图控件，默认的三个控件都不显示
    controls: defaultControls().extend([
      new FullScreen(),
      // new ol.Control.MousePosition(),
      // new ol.Control.OverviewMap(),
      new ScaleLine(),
      //   new ZoomSlider(),
      // new ol.Control.ZoomToExtent()
    ]),
    layers: [TianMapLayer],
    // 地图视图
    view: new View({
      projection: 'EPSG:4326', // 坐标系，有EPSG:4326和EPSG:3857
      // 自定义
      //地图中心点
      center: [118.81, 31.99],
      //   minZoom: 10, // 地图缩放最小级别
      zoom: 9, // 地图缩放级别（打开页面时默认级别）
    }),
  });
  clearMap();
}
export { initMap, map };
