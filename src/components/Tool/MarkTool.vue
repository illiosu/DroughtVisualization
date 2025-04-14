<template>
  <div class="mark-container">
    <div style="width: 350px; font-size: 18px">图形标记</div>
    <div style="position: absolute; cursor: pointer; top: 1px; right: 5px" @click="cancel">X</div>
    <a-form :label-col="{ span: 6 }" :wrapper-col="{ span: 18 }" autocomplete="off" class="form">
      <a-form-item label="标记类型">
        <a-select v-model:value="markerSelect" @change="handleChange">
          <a-select-option value="1">图标标记</a-select-option>
          <!-- <a-select-option value="2">线段标记</a-select-option>
          <a-select-option value="3">图形标记</a-select-option>
          <a-select-option value="4">文字标记</a-select-option> -->
        </a-select>
      </a-form-item>
      <a-form-item label="图标类型">
        <a-radio-group v-model:value="markerRadio">
          <a-radio value="0"><img :src="flag_icon" alt="" class="img-item" /></a-radio>
          <a-radio value="1"><img :src="location_icon" alt="" class="img-item" /></a-radio>
          <a-radio value="2"><img :src="sight_icon" alt="" class="img-item" /></a-radio>
          <a-radio value="3"><img :src="star_icon" alt="" class="img-item" /></a-radio>
        </a-radio-group>
      </a-form-item>
      <a-form-item label="绘制标记">
        <div class="flex-box" v-show="disabled">
          <close-circle-filled :style="{ fontSize: '18px', color: '#F56D6D' }" />
          <div class="fail">未绘制</div>
          <Button type="primary" @click="draw">点击绘制</Button>
        </div>
        <div class="flex-box" v-show="!disabled">
          <check-circle-filled :style="{ fontSize: '18px', color: '#67C23A' }" />
          <div class="success">已绘制</div>
          <Button @click="clear">清除</Button>
        </div>
      </a-form-item>
      <a-form-item label="标记文本">
        <a-input v-model:value="inputVal" />
      </a-form-item>
      <a-form-item label="字体大小">
        <a-select v-model:value="fontSizeSelect">
          <a-select-option value="14px">14px</a-select-option>
          <a-select-option value="16px">16px</a-select-option>
          <a-select-option value="18px">18px</a-select-option>
          <a-select-option value="20px">20px</a-select-option>
        </a-select>
      </a-form-item>
    </a-form>
    <div class="btns-container">
      <a-space wrap>
        <a-button :type="disabled ? undefined : 'primary'" @click="handleOk" :disabled="disabled">确定</a-button>
        <a-button @click="cancel">取消</a-button>
        <a-button @click="clearAll">删除</a-button>
      </a-space>
    </div>
  </div>
</template>

<script setup>
import { ref, onUnmounted } from 'vue';
import { Button, Space, Form, Input } from 'ant-design-vue';
import flag_icon from '@/assets/tool/flag_icon.png';
import location_icon from '@/assets/tool/location_icon.png';
import sight_icon from '@/assets/tool/sight_icon.png';
import star_icon from '@/assets/tool/star_icon.png';
// import { CheckCircleFilled, CloseCircleFilled } from '@ant-design/icons-vue';
// import { map } from '../mapInfoObj/initMap';

const emits = defineEmits(['hideMarkPanel']);
const ButtonGroup = Button.Group;
const markerSelect = ref('1');
const markerRadio = ref('0');
const disabled = ref(true);
const inputVal = ref('');
const fontSizeSelect = ref('14px');
const typeToMarker = ['iconMarker', 'lineMarker', 'shapeMarker', 'textMarker'];
const iconMarker = [flag_icon, location_icon, sight_icon, star_icon];

const draw = () => {
  drawStart({ type: typeToMarker[markerSelect.value - 1] });
};
const drawStart = (drawObj) => {
  startMarker(drawObj);
};
// const startMarker = (param) => {
//   const Lmap = mapObj.map
//   if (!Lmap) return
//   switch (param.type) {
//     case "iconMarker":
//       iconMarkerStart(Lmap);
//       break;
//     case "lineMarker":
//       lineMarkerStart(Lmap);
//       break;
//     case "shapeMarker":
//       if (!param.drawType) break;
//       drawShapeMaker(Lmap, param.drawType);
//       break;
//   }
// }
let currentToolType = null;
// const featureLayerGroup = L.featureGroup();
// featureLayerGroup.addTo(mapObj.map)

let currentDrawLayerId = -1;
const iconMarkerStart = (map) => {
  if (currentToolType) {
    currentToolType.disable();
    currentToolType = null;
  }
  let icon = new L.Draw.Marker(map);
  icon.enable();
  currentToolType = icon;
  map.off(L.Draw.Event.CREATED);
  map.on(L.Draw.Event.CREATED, (e) => {
    e.layer.markerType = 'iconMarker';
    featureLayerGroup.addLayer(e.layer);
    currentDrawLayerId = featureLayerGroup.getLayerId(e.layer);
  });
  map.on(L.Draw.Event.DRAWSTOP, (e) => {
    icon.disable();
    disabled.value = false;
  });
};
const clear = () => {
  clearDraw();
};

const clearDraw = () => {
  cancelCurrentMarker();
};
const cancelCurrentMarker = () => {
  if (currentDrawLayerId && currentDrawLayerId >= 0) {
    featureLayerGroup.removeLayer(currentDrawLayerId);
    currentDrawLayerId = -1;
  }
  if (currentToolType) {
    currentToolType.disable();
    currentToolType = null;
  }
  disabled.value = true;
};
const handleOk = () => {
  if (markerSelect.value == 1) {
    drawEnd({
      type: typeToMarker[markerSelect.value - 1],
      icon: iconMarker[parseInt(markerRadio.value)],
      text: inputVal.value,
      fontSize: fontSizeSelect.value,
    });
  } else if (markerSelect.value == 2) {
    drawEnd({
      type: typeToMarker[markerSelect.value - 1],
      lineDash: lineRadio.value,
      arrowType: parseInt(arrowType.value),
      color: lineColor.value,
    });
  } else if (markerSelect.value == 3) {
    drawEnd({
      type: typeToMarker[markerSelect.value - 1],
      content: inputVal.value,
      fontSize: fontSizeSelect.value,
      fillColor: regionColor.value,
      color: textColor.value,
    });
  } else if (markerSelect.value == 4) {
    drawEnd({
      type: typeToMarker[markerSelect.value - 1],
      content: inputVal.value,
      fontSize: fontSizeSelect.value,
      color: textColor.value,
    });
  }
  inputVal.value = '';
};
const drawEnd = (param) => {
  endMarker(param);
};
const endMarker = (param) => {
  const Lmap = mapObj.map;
  if (!Lmap) return;
  switch (param.type) {
    case 'iconMarker':
      if (!param.icon || !param.fontSize) break;
      iconMarkerEnd(param.icon, param.text || '', param.fontSize);
      break;
    case 'lineMarker':
      if (!param.color) break;
      lineMarkerEnd(Lmap, param.lineDash || '', param.arrowType || 0, param.color);
      break;
    case 'shapeMarker':
      if (!param.fontSize || !param.fillColor || !param.color) break;
      shapeMarkerEnd(param.content || '', param.fontSize, param.fillColor, param.color);
      break;
    case 'textMarker':
      if (!param.fontSize || !param.color) break;
      textMarker(Lmap, param.content || '', param.fontSize, param.color);
      break;
  }
};
const iconMarkerEnd = (icon, text, fontSize) => {
  const textClass = `color:#FF0000;font-size:${fontSize};width:100px;height:20px;text-align:center;word-wrap:break-word`,
    imgClass = `width:40px;height:40px;`,
    iconMakerClass = `width:100px;height:60px;text-align:center;`;
  const customIcon = L.divIcon({
    iconSize: [100, 60],
    iconAnchor: [50, 40],
    className: 'icon-marker',
    html: `<div style="${iconMakerClass}"><img style="${imgClass}" src="${icon}" alt="" /><p  style="${textClass}">${text}</p></div>`,
  });
  const layer = featureLayerGroup.getLayer(currentDrawLayerId);
  if (layer) layer.setIcon(customIcon);
  disabled.value = true;
  currentDrawLayerId = -1;
};

const cancel = () => {
  markerSelect.value = '1';
  disabled.value = true;
  clear();
  emits('hideMarkPanel');
};
const clearAll = () => {
  clearAllMarker();
  cancelAllMarker();
};

const clearAllMarker = () => {
  featureLayerGroup.clearLayers();
};
const cancelAllMarker = () => {
  cancelCurrentMarker();
  featureLayerGroup.clearLayers();
  disabled.value = true;
};

onUnmounted(() => {
  markerSelect.value = '1';
  disabled.value = true;
  clear();
});
</script>

<style scoped>
.mark-container {
  position: absolute;
  top: 65px;
  right: 20px;
  width: 400px;
  height: auto;
  z-index: 450;
  background: #ffffff;
  box-shadow: 0px 2px 6px 0px #c5c5c5;
  padding: 5px;
}

.form {
  margin: 5px 15px;
}

.btns-container {
  margin: 5px 15px;
  text-align: center;
}

.img-item {
  width: 25px;
  height: 25px;
}

:deep(.flex-box) {
  display: flex;
}

.anticon {
  transform: translateY(6px);
}

.success {
  color: #67c23a;
  font-size: 16px;
  font-weight: 550;
  margin: 3px 8px 0 5px;
}

.fail {
  color: #f56c6c;
  font-size: 16px;
  font-weight: 550;
  margin: 3px 8px 0 5px;
}
</style>
