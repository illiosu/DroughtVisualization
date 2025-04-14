<template>
  <div class="layout_container">
    <div class="layout_slider" :class="{ fold: LatOutSettingStore.fold ? true : false }" v-if="!isTabbarClicked">
      <el-scrollbar class="scrollbar">
        <el-menu :collapse="LatOutSettingStore.fold ? true : false" :default-active="$route.path" background-color="#001529" text-color="white">
          <Menu :menuList="userStore.menuRoutes"></Menu>
        </el-menu>
      </el-scrollbar>
    </div>
    <div class="layout_tabbar" :class="{ fold: LatOutSettingStore.fold ? true : false }">
      <!-- <Logo></Logo> -->
      <Tabbar></Tabbar>
    </div>
    <div class="layout_info" v-if="isInfoClicked2">
      <info></info>
    </div>
    <div class="layout_data" v-if="isDataClicked">
      <dataManagement></dataManagement>
    </div>
    <div class="layout_user" v-if="isUserInfoClicked">
      <userInfo></userInfo>
    </div>
    <div class="layout_main" :class="{ fold: LatOutSettingStore.fold ? true : false }" v-if="!isTabbarClicked">
      <FoldIcon></FoldIcon>
      <!-- <ecoassess></ecoassess> -->
      <!-- <Tabbar></Tabbar> -->
      <Main></Main>
    </div>
  </div>
</template>
<script setup lang="ts">
import userUserStore from '@/store/modules/user';
import Logo from './logo/index.vue';
import Menu from './menu/index.vue';
import Main from './main/index.vue';
import { useRoute } from 'vue-router';
import Tabbar from './tabbar/index.vue';
import info from './info/index.vue';
import dataManagement from './data/index.vue';
import userInfo from './userInfo/index.vue';
import FoldIcon from './foldIcon/index.vue';
import useLayOutSettingStore from '@/store/modules/setting';
import ecoassess from '@/components/Echarts/ecoassess/index.vue';
import { ref, provide, inject, watch } from 'vue';

let userStore = userUserStore();
let $route = useRoute();
let LatOutSettingStore = useLayOutSettingStore();
let isInfoClicked = inject('isInfoClicked');
let isInfoClicked2 = ref(false);
let isMainClicked = ref(false);
let isDataClicked = ref(false);
let isTabbarClicked = ref(false);
let isUserInfoClicked = ref(false);
provide('isInfoClicked2', isInfoClicked2);
provide('isMainClicked', isMainClicked);
provide('isDataClicked', isDataClicked);
provide('isTabbarClicked', isTabbarClicked);
provide('isUserInfoClicked', isUserInfoClicked);

console.log(isInfoClicked);
// console.log(userStore.menuRoutes);
</script>
<script lang="ts">
export default {
  name: 'Layout',
};
</script>
<style scoped lang="scss">
.layout_container {
  // background-color: red;
  width: 100%;
  height: 100vh;

  .layout_slider {
    width: $base-slider-width;
    position: fixed;
    bottom: 0;
    height: calc(100vh - $base-tabbar-heigth);
    // height: 100vh;
    background-color: $base-slider-background;
    transition: all 0.3s;

    .scrollbar {
      width: 100%;
      height: calc(90vh - $base-slider-logo-height);
    }
    &.fold {
      width: $base-slider-min-width;
    }
  }

  .layout_tabbar {
    width: 100%;
    // width: calc(100% - $base-slider-width);
    position: fixed;
    height: $base-tabbar-heigth;
    // background-color: aqua;
    top: 0px;
    // left: $base-slider-width;
    transition: all 0.3s;
    &.fold {
      width: calc(100vw - $base-slider-min-width);
      // left: $base-slider-min-width;
    }
  }

  .layout_main {
    position: relative;
    width: calc(100vw - $base-slider-width);
    height: calc(100vh - $base-tabbar-heigth);
    background-color: bisque;
    left: $base-slider-width;
    top: $base-tabbar-heigth;
    // padding: 20px;
    overflow: auto;
    transition: all 0.3s;
    &.fold {
      width: calc(100vw - $base-slider-min-width);
      left: $base-slider-min-width;
    }
  }
  .layout_info {
    position: relative;
    width: 100%;
    height: calc(100vh - $base-tabbar-heigth);
    top: $base-tabbar-heigth;
  }
  .layout_data {
    position: relative;
    width: 100%;
    height: calc(100vh - $base-tabbar-heigth);
    top: $base-tabbar-heigth;
  }
  .layout_user {
    position: relative;
    width: 100%;
    height: calc(100vh - $base-tabbar-heigth);
    top: $base-tabbar-heigth;
  }
}
</style>
