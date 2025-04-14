<template>
  <div class="m-timeline-wrap">
    <div class="m-time-dot">
      <!-- 'm-dot-box'：表示此 div 元素始终会有一个类名为 m-dot-box 的 CSS 类。
{ active: active.value === item.year }：
这是一个 JavaScript 对象，它的键表示类名，键的值（这是一个布尔表达式）决定是否向元素添加这个类。
如果该表达式为true，则添加 active 类，否则不添加。
这里的 active.value === item.year 实际上是检查当前元素代表的年份是否是被选中的年份。 -->
      <div :class="['m-dot-box', { active: active === item.year }]" @click="onClickYear(item.year)" v-for="(item, index) in timelineData" :key="index">
        <p class="u-year">{{ item.year }}</p>
        <div class="m-dot">
          <div class="u-dot"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, nextTick, inject, provide, getCurrentInstance, watch } from 'vue';
import 'ol/ol.css'; // 地图样式
import TileWMS from 'ol/source/TileWMS.js';
import XYZ from 'ol/source/XYZ.js';
import { Map, View } from 'ol'; // 地图实例方法、视图方法
import Tile from 'ol/layer/Tile'; // 瓦片渲染方法
import { map } from '@/views/home/initMap.ts';
import { clearMap } from '@/views/home/clearMap';
// 获取emit函数
const { emit } = getCurrentInstance();

const props = defineProps({
  timelineData: {
    // 时间轴数据
    type: Array,
    required: true,
    default: () => {
      return [];
    },
  },
  activeYear: {
    // 初始选中年份
    type: Number,
    default: 2020,
  },
});
let active = ref('2000');
// console.log(active.value);

const onClickYear = (year) => {
  if (active.value !== year) {
    // console.log(year);
    active.value = year;
    // console.log(year);
    // console.log(active.value);
  }
  clearMap();
  // // 删除旧的瓦片层
  // map.value
  //   .getLayers()
  //   .getArray()
  //   .filter(function (layer) {
  //     // 过滤
  //     return layer.getSource().getUrls()[0] !== 'http://t4.tianditu.com/DataServer?T=vec_w&tk=81e92153f7bd56bf35d42ceacbc4ab95&x={x}&y={y}&l={z}';
  //   })
  //   .forEach(function (layer) {
  //     console.log(layer);
  //     // 删除过滤后的图层
  //     if (layer instanceof Tile) {
  //       map.value.removeLayer(layer);
  //     }
  //   });
  // 创建新的瓦片层
  var newLayer = new Tile({
    source: new TileWMS({
      ratio: 1,
      url: 'http://localhost:8080/geoserver/LE/wms',
      params: {
        LAYERS: 'LE:a' + year + '地类重分类1',
        STYLES: '',
        VERSION: '1.1.1',
        tiled: true,
      },
      serverType: 'geoserver',
    }),
  });
  // 添加新的瓦片层到地图上
  map.value.addLayer(newLayer);
  //   console.log(active.value);
};
// 监听active数据的变化
watch(active, (newValue) => {
  // 在数据改变时触发update:active事件
  emit('update:active', newValue);
});
console.log(props);
</script>

<style lang="scss" scoped>
// @themeColor: #1890ff;
.m-timeline-wrap {
  margin: 150px auto;
  height: 3px;
  background: #8dc6f5;
  .m-time-dot {
    display: flex;
    justify-content: space-around;
    .m-dot-box {
      cursor: pointer;
      text-align: center;
      transform: translateY(14px);
      .u-year {
        font-size: 28px;
        font-weight: 500;
        color: #333;
        transform: translateY(-8px);
        transition: all 0.3s ease;
      }
      .m-dot {
        margin: 0 auto;
        width: 14px;
        height: 14px;
        background: #8dc6f5;
        border-radius: 50%;
        transition: all 0.3s ease;
        .u-dot {
          width: 14px;
          height: 14px;
          background: $timeline-color;
          border-radius: 50%;
          transition: all 0.3s ease;
        }
      }
    }
    .m-dot-box:hover {
      .u-year {
        color: $timeline-color;
      }
      .m-dot {
        .u-dot {
          background: $timeline-color;
        }
      }
    }
    .active {
      .u-year {
        transform: scale(2) translateY(-18px);
        // color: $themeColor;
        color: $timeline-color;
      }
      .m-dot {
        transform: scale(3);
        .u-dot {
          transform: scale(0.67);
          background: $timeline-color;
        }
      }
    }
  }
}
</style>
