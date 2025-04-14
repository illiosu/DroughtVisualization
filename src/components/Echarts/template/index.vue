<template>
  <div class="my_charts">
    <div ref="chartDiv" :style="{ width: width, height: height }"></div>
  </div>

  <!-- <p v-if="isEmpty">暂无数据</p> -->
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch, computed } from 'vue';
import * as echarts from 'echarts';
// import { options1, options2 } from '@/components/Echarts/options/chartOption';
// console.log(options2);
// 使用者通过props传入size, 数据和自定义样式
const props = defineProps({
  customOption: {
    type: Object,
    // default: () => ({ ...options1 }),
  },
  width: { type: String, default: '400px' },
  height: { type: String, default: '400px' },
});

const chartDiv = ref();
let chart;

// 检测数据是否为空
// const isEmpty = computed(() => !props.data || Object.keys(props.data).length === 0);

onMounted(() => {
  // 初始化ECharts实例
  chart = echarts.init(chartDiv.value);

  // 合并默认样式配置和使用者自定义的样式配置
  //   const option = { ...defaultOption, ...props.customOption };
  chart.setOption(props.customOption);

  window.addEventListener('resize', () => {
    if (chart && chart.resize) {
      chart.resize();
    }
  });
});

onBeforeUnmount(() => {
  if (chart && !chart.isDisposed()) {
    chart.dispose();
  }
});

watch(
  () => props.data,
  (newData) => {
    if (chart) {
      const option = chart.getOption();
      chart.setOption(props.customOption);
    }
  },
  { deep: true },
);

watch(
  () => props.customOption,
  (newOption) => {
    if (chart) {
      chart.clear();
      chart.setOption(props.customOption);
    }
  },
  { deep: true },
);
</script>

<style lang="scss" scoped>
.my_charts {
  // background-color: whitesmoke;
  // position: absolute;
  // position: fixed;
  height: 100%;
  width: 100%;
}
</style>
