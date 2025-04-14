<template>
  <div class="documentation">
    <Sidebar :items="items" @select="selectItem"></Sidebar>
    <Content :item="selectedItem"></Content>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import Sidebar from '@/components/Sliderbar/index.vue';
import Content from '@/components/Content/index.vue';

let items = ref([
  {
    title: 'Item 1',
    content: 'Content of Item 1',
  },
  {
    title: 'Item 2',
    content: 'Content of Item 2',
  },
]);

let selectedItem = ref(null);

const selectItem = (item) => {
  selectedItem.value = item;
};
</script>
