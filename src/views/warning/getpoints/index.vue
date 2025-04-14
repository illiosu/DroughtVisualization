<template>
  <div id="map">
    <el-popover
      :width="200"
      :popper-style="{ position: 'absolute', left: `${popPosition.x}px`, top: `${popPosition.y}px` }"
      :visible="visible"
      :title="title"
      placement="left"
      transition="el-zoom-in-left">
      <template #reference>
        <span></span>
      </template>

      <template #default>
        <SvgIcon name="关闭" style="cursor: pointer; position: absolute; right: 0; top: 0" color="green" width="20px" height="20px" @click="visible = !visible"></SvgIcon>

        <div class="demo-rich-conent" style="display: flex; gap: 16px; flex-direction: column">
          <div>
            <p class="demo-rich-content__name" style="margin: 0; font-weight: 500">{{ '预警级别：' + rank }}</p>
            <p class="demo-rich-content__name" style="margin: 0; font-weight: 500">{{ '经度：' + lng }}</p>
            <p class="demo-rich-content__name" style="margin: 0; font-weight: 500">{{ '纬度：' + lat }}</p>
            <p class="demo-rich-content__name" style="margin: 0; font-weight: 500">{{ '预警说明：' + explain }}</p>
          </div>
        </div>
      </template>
    </el-popover>
  </div>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref, watch, provide, inject, nextTick } from 'vue';
import { initMap, map } from '@/views/home/initMap';
import { Feature } from 'ol';
import { Point } from 'ol/geom';
import { fromLonLat } from 'ol/proj';
import Overlay from 'ol/Overlay.js';
import Select from 'ol/interaction/Select';
import { click } from 'ol/events/condition';
import { Style, Icon } from 'ol/style';
import { Tile as TileLayer, Vector as VectorLayer } from 'ol/layer';
import { Vector as VectorSource } from 'ol/source';
import SvgIcon from '@/components/SvgIcon/index.vue';
//@ts-ignore
import ToolsSet from '@/components/Tool/ToolsSet.vue';
import { getPointsApi } from '@/api/test/index.ts';
import exp from 'constants';
let landtype = ref('');
const visible = ref(false);
let title = ref('');
let rank = ref('');
let lng = ref('');
let lat = ref('');
let explain = ref('');
const popPosition = reactive({ x: 118, y: 33 });

onMounted(() => {
  initMap();
  addPoints();
  clickWarnIcon();
  changeCursor();
});
const getPoints = async () => {
  const res = await getPointsApi();
  console.log(res);
};

const addPoints = async () => {
  let iconUrl;
  let iconClickUrl;
  // const res = await getPointsApi();
  // console.log(res);
  let res_lnglat = await getPointsApi();
  // console.log(res_lnglat.data);
  let coorArray = res_lnglat.data
    .filter((item: any) => item !== null && item !== undefined) // 添加过滤操作，排除 null 或 undefined 的项
    .map((item: any) => {
      // console.log(item.土地类型);
      return { lng: item.经度, lat: item.纬度, type: item.土地类型, rank: item.级别, explain: item.说明 };
    });
  // let features = coorArray.map((item) => {
  //   //todo 少用gpt，太智障了
  //   // let coordinate = fromLonLat([parseFloat(item.lng), parseFloat(item.lat)]);
  //   // console.log(coordinate);

  let features = coorArray.map((item: any) => {
    let pointGeom = new Point([item.lng, item.lat]);
    let feature = new Feature({
      geometry: pointGeom,
      type: item.type,
      rank: item.rank,
      explain: item.explain,
    });

    // 判断土地类型并设置图标

    switch (item.rank) {
      case '普通预警':
        iconUrl = '/public/tools/普通预警.png';
        break;
      case '高级预警':
        iconUrl = '/public/tools/高级预警.png';
        break;
      default:
        iconUrl = '/public/tools/工具.png';
    }
    feature.setStyle(
      new Style({
        image: new Icon({
          src: iconUrl,
          scale: 0.5, // 调整图标的大小
        }),
      }),
    );
    // console.log(item.type);

    return feature;
  });

  let vectorSource = new VectorSource({
    features: features,
  });

  let vectorLayer = new VectorLayer({
    source: vectorSource,
  });

  map.value.addLayer(vectorLayer);
};
// 更改鼠标移动到图标上时的样式，
const changeCursor = () => {
  let defaultCursor = map.value.getTargetElement().style.cursor;
  // 添加pointermove监听事件
  map.value.on('pointermove', function (evt) {
    // pixel 鼠标当前位置的数组 [x, y]，
    let pixel = map.value.getEventPixel(evt.originalEvent);
    // 选择范围
    let hit = map.value.hasFeatureAtPixel(pixel, { hitTolerance: 10 });

    if (hit) {
      map.value.getTargetElement().style.cursor = 'pointer';
    } else {
      map.value.getTargetElement().style.cursor = defaultCursor;
    }
  });
};
/**
 * 保留小数位数
 * value: 数据源
 * n: 保留几位小数
 */
const retain = (value: any, n: any) => {
  if (n === 'null' || n === 'undefined' || n === 0) return parseInt(value);
  let tran = Math.round(value * Math.pow(10, n)) / Math.pow(10, n);
  let tranV = tran.toString();
  let newVal = tranV.indexOf('.');
  if (newVal < 0) {
    tranV += '.';
  }
  for (let i = tranV.length - tranV.indexOf('.'); i <= n; i++) {
    tranV += '0';
  }
  return tranV;
};

const clickWarnIcon = () => {
  let iconUrl;
  console.log('clickFeature');

  var selectSingleClick = new Select({
    hitTolerance: 10, // 选择容差
    style: function (feature) {
      // console.log(feature.values_.type);
      //@ts-ignore
      switch (feature.values_.rank) {
        case '普通预警':
          iconUrl = '/public/tools/普通预警.png';
          break;
        case '高级预警':
          iconUrl = '/public/tools/高级预警.png';
          break;
        default:
          iconUrl = '/public/tools/工具.png';
      }
      return new Style({
        image: new Icon({
          src: iconUrl,
          scale: 0.5,
        }),
      });
    },
  });
  map.value.addInteraction(selectSingleClick);
  // 设置上一次选中的 feature
  let lastSelected = null;
  selectSingleClick.on('select', function (e) {
    console.log('select');
    visible.value = true;
    // visible.value = !visible.value; // 点击时切换可见性

    var selected = e.selected[0]; // 获取选中的 feature
    // console.log(selected);
    if (selected === undefined) {
      return;
    }

    // if (selected.get('type') === undefined) {
    //   return;
    // }
    var type = selected.get('type'); // 获取土地类型
    landtype.value = type;
    title.value = type;
    rank.value = selected.get('rank');
    explain.value = selected.get('explain');
    //@ts-ignore
    let lng_fixed = retain(selected.getGeometry().getCoordinates()[0], 3);
    //@ts-ignore
    let lat_fixed = retain(selected.getGeometry().getCoordinates()[1], 3);
    // console.log(coordinate);
    //@ts-ignore
    lng.value = lng_fixed;
    //@ts-ignore
    lat.value = lat_fixed;

    var coords = e.mapBrowserEvent.coordinate;
    var pixel = map.value.getPixelFromCoordinate(coords);
    // console.log(pixel);
    // 更新popover的位置
    popPosition.x = pixel[0];
    popPosition.y = pixel[1];
    // resetPopover(pixel);
    // selected.on('change', () => {});
  });
};
// const resetPopover = (pixel) => {
//   popPosition.x = pixel[0];
//   popPosition.y = pixel[1];

//   // 设置visible为false，销毁popover
//   visible.value = false;

//   // 下一个Vue的渲染周期，重新创建popover
//   nextTick(() => {
//     visible.value = true;
//   });
// };
</script>
<style lang="scss" scoped>
#map {
  width: 100%;
  height: 100%;
}
</style>
