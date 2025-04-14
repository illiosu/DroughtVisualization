import { map } from '@/views/home/initMap.ts';
import Tile from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';

const clearMap = () => {
  if (map.value && map.value.getLayers) {
    // 清除图层
    map.value
      .getLayers()
      .getArray()
      .filter(function (layer: any) {
        // 过滤
        if (typeof layer.getSource().getUrls === 'function') {
          return layer.getSource().getUrls()[0] !== 'http://t4.tianditu.com/DataServer?T=vec_w&tk=81e92153f7bd56bf35d42ceacbc4ab95&x={x}&y={y}&l={z}';
        }
        return true; // 如果源没有 getUrls 方法，我们保留这个图层
      })
      .forEach(function (layer: any) {
        // 删除过滤后的图层
        if (layer instanceof Tile || layer instanceof VectorLayer) {
          console.log('removeLayer');
          map.value.removeLayer(layer);
        }
      });
  } else {
    console.log('map.value or map.value.getLayers is not defined');
  }
};
const clearShpMap = () => {
  if (map.value && map.value.getLayers) {
    // 清除图层
    map.value
      .getLayers()
      .getArray()
      .forEach(function (layer: any) {
        // 删除过滤后的图层
        if (layer instanceof VectorLayer) { // Only remove layers that are instances of VectorLayer 
          console.log('removeShpLayer');
          map.value.removeLayer(layer);
        }
      });
  } else {
    console.log('map.value or map.value.getLayers is not defined');
  }
};
const removeSpecificLayer = (layerName: string) => {
  if (map.value && map.value.getLayers) {
    // 获取图层
    const layers = map.value.getLayers().getArray();
    
    // 查找指定的图层
    const specificLayer = layers.find((layer: any) => layer.get('name') === layerName);

    // 如果找到了指定的图层
    if (specificLayer) {
      console.log(`removeLayer: ${layerName}`);
      map.value.removeLayer(specificLayer);  // 删除这个图层
    } else {
      console.log(`Layer: ${layerName} not found`);
    }
  } else {
    console.log('map.value or map.value.getLayers is not defined');
  }
};
export { clearMap,clearShpMap,removeSpecificLayer};
