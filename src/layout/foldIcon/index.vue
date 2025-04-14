<template>
  <el-icon style="margin-right: 10px" @click="changeIcon" class="fold_icon">
    <component :is="LayOutSettingStore.fold ? 'Fold' : 'Expand'"></component>
  </el-icon>
  <!-- <el-breadcrumb separator-icon="ArrowRight">
      <el-breadcrumb-item v-for="(item, index) in $route.matched" :key="index" v-show="item.meta.title" :to="item.path">
        <el-icon>
          <component :is="item.meta.icon"></component>
        </el-icon>
      </el-breadcrumb-item>
    </el-breadcrumb> -->
</template>
<script setup lang="ts">
import useLayOutSettingStore from '@/store/modules/setting';
let LayOutSettingStore = useLayOutSettingStore();
const changeIcon = () => {
  LayOutSettingStore.fold = !LayOutSettingStore.fold;
};
</script>
<script lang="ts">
export default {
  name: 'Breadcrumb',
};
</script>
<style scoped lang="scss">
.fold_icon {
  position: absolute;
  z-index: 100;
}
</style>
