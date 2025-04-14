<template>
  <div class="content">
    <div v-if="item">
      <h1>{{ item.title }}</h1>
      <p>{{ item.content }}</p>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';

const item = ref(null);
</script>
