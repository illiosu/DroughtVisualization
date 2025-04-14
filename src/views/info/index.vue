<template>
  <div id="info-container">
    <el-row style="height: 100%">
      <el-col :span="4">
        <el-anchor :container="containerRef" direction="vertical" type="default" :offset="30" @click="handleClick">
          <el-anchor-link href="#part1" title="part1" />
          <el-anchor-link href="#part2" title="part2" />
          <el-anchor-link href="#part3" title="part3" />
        </el-anchor>
      </el-col>
      <el-col :span="20">
        <div ref="containerRef" style="height: 100%; overflow-y: auto">
          <div id="part1" style="height: 100%; margin-top: 30px">part1</div>
          <div id="part2" style="height: 100%; margin-top: 30px">part2</div>
          <div id="part3" style="height: 100%; margin-top: 30px">part3</div>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';

const containerRef = ref<HTMLElement | null>(null);
// containerRef.value = document.getElementById('info-container');
const handleClick = (e: MouseEvent) => {
  e.preventDefault();
};
</script>
<style lang="scss" scoped>
#info-container {
  width: 100%;
  height: 100%;
}
</style>
