<template>
  <!-- 为 ECharts 准备一个定义了宽高的 DOM -->
  <div class="main">
    <div id="panelchart" class="panenl_chart"></div>
    <div id="barchart" class="bar_chart"></div>
  </div>
</template>

<script setup lang="ts">
import { defineComponent, nextTick, onMounted, getCurrentInstance } from 'vue';
const { proxy } = getCurrentInstance() as any;

onMounted(() => {
  // 指定图表的配置项和数据
  var paneloption = {
    series: [
      {
        type: 'gauge',
        startAngle: 180,
        endAngle: 0,
        center: ['50%', '75%'],
        radius: '90%',
        min: 0,
        max: 1,
        splitNumber: 8,
        axisLine: {
          lineStyle: {
            width: 6,
            color: [
              [0.25, '#FF6E76'],
              [0.5, '#FDDD60'],
              [0.75, '#58D9F9'],
              [1, '#7CFFB2'],
            ],
          },
        },
        pointer: {
          icon: 'path://M12.8,0.7l12,40.1H0.7L12.8,0.7z',
          length: '12%',
          width: 20,
          offsetCenter: [0, '-60%'],
          itemStyle: {
            color: 'auto',
          },
        },
        axisTick: {
          length: 12,
          lineStyle: {
            color: 'auto',
            width: 2,
          },
        },
        splitLine: {
          length: 20,
          lineStyle: {
            color: 'auto',
            width: 5,
          },
        },
        axisLabel: {
          color: '#464646',
          fontSize: 20,
          distance: -60,
          rotate: 'tangential',
          formatter: function (value) {
            if (value === 0.875) {
              return 'Grade A';
            } else if (value === 0.625) {
              return 'Grade B';
            } else if (value === 0.375) {
              return 'Grade C';
            } else if (value === 0.125) {
              return 'Grade D';
            }
            return '';
          },
        },
        title: {
          offsetCenter: [0, '-10%'],
          fontSize: 20,
        },
        detail: {
          fontSize: 30,
          offsetCenter: [0, '-35%'],
          valueAnimation: true,
          formatter: function (value) {
            return Math.round(value * 100) + '';
          },
          color: 'inherit',
        },
        data: [
          {
            value: 0.7,
            name: 'Grade Rating',
          },
        ],
      },
    ],
  };
  var baroption = {
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        // Use axis to trigger tooltip
        type: 'shadow', // 'shadow' as default; can also be 'line' or 'shadow'
      },
    },
    legend: {},
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true,
    },
    xAxis: {
      type: 'value',
    },
    yAxis: {
      type: 'category',
      data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
    },
    series: [
      {
        name: 'Direct',
        type: 'bar',
        stack: 'total',
        label: {
          show: true,
        },
        emphasis: {
          focus: 'series',
        },
        data: [320, 302, 301, 334, 390, 330, 320],
      },
      {
        name: 'Mail Ad',
        type: 'bar',
        stack: 'total',
        label: {
          show: true,
        },
        emphasis: {
          focus: 'series',
        },
        data: [120, 132, 101, 134, 90, 230, 210],
      },
      {
        name: 'Affiliate Ad',
        type: 'bar',
        stack: 'total',
        label: {
          show: true,
        },
        emphasis: {
          focus: 'series',
        },
        data: [220, 182, 191, 234, 290, 330, 310],
      },
      {
        name: 'Video Ad',
        type: 'bar',
        stack: 'total',
        label: {
          show: true,
        },
        emphasis: {
          focus: 'series',
        },
        data: [150, 212, 201, 154, 190, 330, 410],
      },
      {
        name: 'Search Engine',
        type: 'bar',
        stack: 'total',
        label: {
          show: true,
        },
        emphasis: {
          focus: 'series',
        },
        data: [820, 832, 901, 934, 1290, 1330, 1320],
      },
    ],
  };
  nextTick(() => {
    const chartDom = <HTMLElement>document.getElementById('panelchart');
    const panelChart = proxy.$echarts.init(chartDom);
    panelChart.setOption(paneloption);

    const barDom = <HTMLElement>document.getElementById('barchart');
    const barChart = proxy.$echarts.init(barDom);
    barChart.setOption(baroption);
    window.onresize = function () {
      panelChart.resize();
      barChart.resize();
    };
  });
});
</script>

<style lang="scss" scoped>
.panenl_chart {
  width: 300px;
  height: 200px;
}
.bar_chart {
  width: 300px;
  height: 200px;
}
.main {
  //   overflow: hidden;
  background-color: whitesmoke;
  top: 100px;
  left: 350px;
  position: fixed;
  z-index: 999;
  /* 半透明 */
  //   opacity: 0.7;
  background-color: rgba(0, 0, 0, 0.3);
  background-repeat: no-repeat;
  border-radius: 15px;
  //   display: flex;
  //   justify-content: space-around;
}
</style>
