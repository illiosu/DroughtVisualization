<template>
  <!-- <el-menu mode="horizontal" class="my-menu">
    <el-menu-item> 菜单一 </el-menu-item>
    <el-menu-item> 菜单二 </el-menu-item>
    <el-menu-item> 菜单三 </el-menu-item>
    <el-menu-item> 菜单四 </el-menu-item>
  </el-menu> -->
  <el-menu :default-active="activeIndex" class="el-menu-demo" mode="horizontal" :ellipsis="false" @select="handleSelect">
    <el-menu-item index="0">
      <img :src="setting.logo" alt="Element logo" class="logo" style="margin-right: 10px" />
      <p>{{ setting.title }}</p>
    </el-menu-item>

    <div class="flex-grow" />
    <el-menu-item index="1" @click="mainClick">评价系统</el-menu-item>
    <!-- <el-menu-item index="2"><div @click="navigateToData">数据管理</div></el-menu-item> -->
    <!-- <el-menu-item index="2"><router-link class="no-underline" to="/data">数据管理</router-link></el-menu-item> -->
    <el-menu-item index="2" @click="dataClick">数据管理</el-menu-item>

    <!-- <a href="http://127.0.0.1:5174/">用户手册</a> -->

    <el-menu-item index="3" @click="infoClick">快速上手</el-menu-item>
    <el-menu-item index="4" @click="userClick">用户中心</el-menu-item>
    <div class="flex-grow" />
    <div class="avatar-container">
      <el-avatar src="https://cube.elemecdn.com/0/88/03b0d39583f48206768a7534e55bcpng.png" />
    </div>
    <!-- <el-menu-item index="4">Processing Center</el-menu-item> -->
    <!-- <el-menu-item index="5">Processing Center</el-menu-item> -->
  </el-menu>
</template>
<script setup lang="ts">
import setting from '@/setting';
import { ref, provide, inject } from 'vue';
import { useRouter } from 'vue-router';

let isInfoClicked = ref(false);
provide('isInfoClicked', isInfoClicked);
let isInfoClicked2 = inject('isInfoClicked2');
let isMainClicked = inject('isMainClicked');
let isDataClicked = inject('isDataClicked');
let isTabbarClicked = inject('isTabbarClicked');
let isUserInfoClicked = inject('isUserInfoClicked');
console.log(isInfoClicked2.value);
// import { test } from '@/api/test/index.js';
const activeIndex = ref('1');
const handleSelect = (key: string, keyPath: string[]) => {
  // console.log(key, keyPath);
};
// const tt: any = () => {
//   var data = test();
//   console.log(data);
// };
const goToLink = () => {
  window.location.href = 'http://127.0.0.1:5174/';
};
const navigateToData = () => {
  console.log('navigateToData');
  const router = useRouter();
  router.push('/data');
};
const infoClick = () => {
  console.log('infoClick');
  isTabbarClicked.value = true;
  isInfoClicked2.value = true;
  isDataClicked.value = false;
  isUserInfoClicked.value = false;
};
const mainClick = () => {
  isTabbarClicked.value = false;
  isInfoClicked2.value = false;
  isDataClicked.value = false;
  isUserInfoClicked.value = false;
};
const dataClick = () => {
  console.log('dataClick');
  isTabbarClicked.value = true;
  isDataClicked.value = true;
  isInfoClicked2.value = false;
  isUserInfoClicked.value = false;
};
const userClick = () => {
  console.log('userClick');
  isTabbarClicked.value = true;
  isUserInfoClicked.value = true;
  isInfoClicked2.value = false;
  isDataClicked.value = false;
};
</script>
<style scoped lang="scss">
.no-underline {
  text-decoration: none;
}
// .my-menu {
//   display: flex;
//   justify-content: space-between;
// }
.flex-grow {
  flex-grow: 0.1;
}
.logo {
  width: $base-logo-width;
}
.el-menu-demo {
  justify-content: space-between;
  display: flex;
}
.avatar-container {
  display: flex;
  left: 2vw;
  align-items: center;
  height: 100%;
}
</style>
