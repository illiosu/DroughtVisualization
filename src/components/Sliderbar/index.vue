<template>
  <div class="sidebar">
    <ul>
      <li v-for="item in items" :key="item.title" @click="selectItem(item)">
        {{ item.title }}
      </li>
    </ul>
  </div>
</template>

<script setup>
import { ref } from 'vue';

const items = ref([]);

const selectItem = (item) => {
  emit('select', item);
};
</script>
