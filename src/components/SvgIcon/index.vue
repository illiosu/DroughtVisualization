<template>
  <svg :style="{ width, height }">
    <!-- svg改不了颜色：删掉svg的fill -->
    <use :xlink:href="prefix + name" :fill="color"></use>
  </svg>
</template>
<script setup lang="ts">
defineProps({
  //xlink:href属性值前缀
  prefix: {
    type: String,
    default: '#icon-',
  },
  //提供使用的图标的名字
  name: String,
  //接收父组件传递的颜色
  color: {
    type: String,
    default: '',
  },
  //接收宽度
  width: {
    type: String,
    default: '16px',
  },
  height: {
    type: String,
    default: '16px',
  },
});
</script>
<style lang="scss" scoped></style>
