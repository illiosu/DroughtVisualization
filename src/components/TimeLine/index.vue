<template>
  <div class="time-line">
    <SvgIcon name="箭头上" style="cursor: pointer; position: absolute; right: 75%; top: -20%" color="green" width="20px" height="20px" @click="handleDownClick"></SvgIcon>

    <el-timeline>
      <el-timeline-item v-for="(activity, index) in activities" :key="index" :color="index === activeActivityIndex ? activityColor : ''" :class="{ 'is-active': index === activeActivityIndex }">
        {{ activity.content }}
      </el-timeline-item>
    </el-timeline>
    <SvgIcon name="箭头下" style="cursor: pointer; position: absolute; right: 75%; bottom: 10%" color="green" width="20px" height="20px" @click="handleDownClick"></SvgIcon>

    <div>
      <SvgIcon
        style="cursor: pointer; position: absolute; right: 65%; top: 98%"
        width="30px"
        height="30px"
        v-if="isPlaying"
        name="暂停"
        :class="{ 'click-animation': isPlaying }"
        @click="handlePlayButtonClick"></SvgIcon>
      <SvgIcon
        style="cursor: pointer; position: absolute; right: 65%; top: 98%"
        width="30px"
        height="30px"
        v-else
        name="播放"
        :class="{ 'click-animation': !isPlaying }"
        @click="handlePlayButtonClick"></SvgIcon>
    </div>

    <!-- <el-button @click="handlePlayButtonClick">Next</el-button> -->
  </div>
</template>

<script lang="ts" setup>
import { ref, getCurrentInstance, watch } from 'vue';
import { map } from '@/views/home/initMap.ts';
import { clearMap } from '@/views/home/clearMap';
import Tile from 'ol/layer/Tile'; // 瓦片渲染方法
import TileWMS from 'ol/source/TileWMS.js';
// import { initMap } from '@/views/home/initMap.ts';
import SvgIcon from '@/components/SvgIcon/index.vue';
//@ts-ignore
const { emit } = getCurrentInstance();

const activeActivityIndex = ref(0);

const activeYear = ref('2000');

const activityColor = '#0bbd87';

let isPlay = false;
const isPlaying = ref(false);
let IconWidth = '55px';
let IconHeight = '55px';
// const togglePlay = () => {};
const activities = [
  {
    content: '2000',
    timestamp: '2018-04-15',
    color: '#0bbd87',
  },
  {
    content: '2010',
    timestamp: '2018-04-13',
  },
  {
    content: '2020',
    timestamp: '2018-04-11',
  },
];

let timer: any = null;

// test();
const handlePlayButtonClick = () => {
  isPlaying.value = !isPlaying.value;
  if (!isPlay) {
    // 如果没有在播放，就开始播放
    timer = setInterval(() => {
      if (activeActivityIndex.value < activities.length - 1) {
        activeActivityIndex.value++;
      } else {
        activeActivityIndex.value = 0;
      }
    }, 2000);
    isPlay = true;
  } else {
    clearInterval(timer);
    timer = null;
    isPlay = false;
  }
};
const handleUpClick = () => {
  if (activeActivityIndex.value > 0) {
    activeActivityIndex.value--;
  } else {
    activeActivityIndex.value = activities.length - 1;
  }
};

const handleDownClick = () => {
  if (activeActivityIndex.value < activities.length - 1) {
    activeActivityIndex.value++;
  } else {
    activeActivityIndex.value = 0;
  }
};
watch(activeActivityIndex, (newIndex) => {
  const activeActivity = activities[newIndex];
  if (activeActivity) {
    activeYear.value = activeActivity.content;
  }
  emit('update:active', activeYear.value);

  switchYear(activeYear.value, map);
  // console.log(activeActivityContent.value);
});
const switchYear = (activeYear: string, map: any) => {
  // console.log('update:active');
  console.log(activeYear);
  clearMap();

  var geoserverLayer = new Tile({
    source: new TileWMS({
      //@ts-ignore
      ratio: 1,
      url: 'http://localhost:8080/geoserver/NanJing/wms',
      params: {
        LAYERS: 'NanJing:' + activeYear + '年地类重分类',
        STYLES: '',
        VERSION: '1.1.1',
        tiled: true,
      },
      serverType: 'geoserver',
    }),
  });
  // 添加新的瓦片层到地图上
  map.value.addLayer(geoserverLayer);
};
</script>

<style lang="scss" scoped>
.time-line {
  width: 100%;
  height: 100%;
  // background-color: blue;
  // color: rgb(125, 50, 50);
}
@keyframes click {
  0% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.1);
  }
  100% {
    transform: scale(1);
  }
}

.click-animation {
  animation: click 0.2s;
}
</style>
