<template>
  <div class="logo">
    <img :src="setting.logo" alt="" />
    <p>{{ setting.title }}</p>
  </div>
</template>
<style lang="scss" scoped>
.logo {
  color: rgb(0, 0, 0);
  display: flex;
  align-items: center;
  position: fixed;
  // padding: 20px;

  img {
    width: $base-logo-width;
    height: $base-slider-logo-height;

    // width: if( LatOutSettingStore , cyan, green);
  }

  p {
    font-size: $base-logo-title-fontSize;
    margin-left: 10px;
  }
}
</style>
<script setup lang="ts">
import setting from '@/setting';
import useLayOutSettingStore from '@/store/modules/setting';
let LayOutSettingStore = useLayOutSettingStore();
// console.log(LayOutSettingStore.fold);
</script>
