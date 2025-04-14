<template>
  <div>
    <div class="form-container" style="position: absolute; top: 50px; left: 50px; z-index: 1000">
      <el-dialog v-model="isShowWarningForm" title="敏感点预警" width="500" :before-close="handleClose" @open="handleOpen()">
        <el-form ref="ruleFormRef" style="max-width: 600px" :model="warningForm" :rules="rules" label-width="auto" class="demo-ruleForm" :size="formSize" status-icon>
          <el-form-item label="预警地类" prop="landtype" required>
            <el-select v-model="warningForm.landtype" placeholder="Activity zone">
              <el-option label="耕地" value="耕地" />
              <el-option label="林地" value="林地" />
              <el-option label="草地" value="草地" />
              <el-option label="水域" value="水域" />
              <el-option label="建设用地" value="建设用地" />
              <el-option label="未利用地" value="未利用地" />
            </el-select>
          </el-form-item>
          <el-form-item label="坐标经度" prop="lng" required>
            <el-input v-model="warningForm.lng" />
          </el-form-item>

          <el-form-item label="坐标纬度" prop="lat" required>
            <el-input v-model="warningForm.lat" />
          </el-form-item>
          <!-- <el-form-item label="Activity zone" prop="region">
            <el-select v-model="ruleForm.region" placeholder="Activity zone">
              <el-option label="Zone one" value="shanghai" />
              <el-option label="Zone two" value="beijing" />
            </el-select>
          </el-form-item> -->
          <!-- <el-form-item label="Activity count" prop="count">
            <el-select-v2 v-model="ruleForm.count" placeholder="Activity count" :options="options" />
          </el-form-item> -->
          <el-form-item label="预警时间" required>
            <el-col :span="11">
              <el-form-item prop="date1">
                <el-date-picker v-model="warningForm.date1" type="date" label="Pick a date" placeholder="Pick a date" style="width: 100%" />
              </el-form-item>
            </el-col>
            <el-col class="text-center" :span="2">
              <span class="text-gray-500">-</span>
            </el-col>
            <el-col :span="11">
              <el-form-item prop="date2">
                <el-time-picker v-model="warningForm.date2" label="Pick a time" placeholder="Pick a time" style="width: 100%" />
              </el-form-item>
            </el-col>
          </el-form-item>

          <el-form-item label="预警级别" prop="rank" required>
            <el-select v-model="warningForm.rank" placeholder="Activity zone">
              <el-option label="普通预警" value="普通预警" />
              <el-option label="高级预警" value="高级预警" />
            </el-select>
          </el-form-item>
          <el-form-item label="预警说明" prop="explain" required>
            <el-input v-model="warningForm.explain" placeholder="请输入预警说明"></el-input>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="submitForm(ruleFormRef)"> 创建 </el-button>
            <el-button @click="resetForm(ruleFormRef)">重置</el-button>
          </el-form-item>
        </el-form>
      </el-dialog>
    </div>
  </div>
</template>
<script setup lang="ts">
import { onMounted, reactive, ref, watch, inject, provide, watchEffect, vModelRadio } from 'vue';

import type { FormInstance, FormRules } from 'element-plus';
import { addPointApi } from '@/api/test/index.ts';
import { ElMessageBox } from 'element-plus';
import mitt from 'mitt';
// import emitter from '@/utils/mitt.ts';
// import eventBus from '@/utils/eventBus.ts';
import { Loading } from '@element-plus/icons-vue';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import Feature from 'ol/Feature.js';
import Point from 'ol/geom/Point.js';
import { Style, Icon } from 'ol/style';
import { initMap, map } from '@/views/home/initMap';
console.log('warningForm');
let testpro = inject('testpro');
// console.log('testpro', testpro);
const isShowWarningForm: any = inject('isShowWarningForm');
const lngForm: any = inject('lngForm');
const latForm: any = inject('latForm');
// const warnrank: any = inject('warnrank');
// const isWarningFormOpen: any = inject('isWarningFormOpen');
// const landType = ref('');
// console.log('props', props.isShow);

interface RuleForm {
  landtype: string;
  lng: string;
  lat: string;
  date1: string;
  date2: string;
  rank: string;
  explain: string;
}
const dialogVisible = ref(false);

const formSize = ref('default');
const ruleFormRef = ref<FormInstance>();
var vectorSource = new VectorSource();
const warningForm = reactive<RuleForm>({
  landtype: '',
  lng: '',
  lat: '',
  date1: '',
  date2: '',
  rank: '',
  explain: '',
});
// const lng = ref('111');
onMounted(() => {
  // console.log('WarningForm onMounted');
  // console.log('lngForm', lngForm.value);
  warningForm.lng = lngForm.value.toString();
  warningForm.lat = latForm.value.toString();
  // eventBus.on('coor-selected', (coordinates: any) => {
  //   // console.log('Coordinates received:', coordinates.lng);
  //   // console.log('warningForm before update:', warningForm);

  //   warningForm.lng = coordinates.lng.toString();

  //   // console.log('warningForm after update:', warningForm);
  // });
  // console.log(lng.value);
  // lng.value = '3333';
});
// onMounted(() => {
//   // console.log(lng);
//   // // 调用
//   // emitter.emit('foo', 'emitter');
//   eventBus.on('coor-selected', (coordinates) => {
//     console.log(warningForm.lng);
//     console.log(coordinates.lng);
//     warningForm.lng = '3333';
//     console.log(warningForm.lng);
//   });
// });
watchEffect(() => {});
// console.log(warningForm.date2);
const locationOptions = ['Home', 'Company', 'School'];
// eventBus.on('coor-selected', (coordinates) => {
//   console.log(coordinates);
//   warningForm.value.lng = coordinates.lng;
//   warningForm.value.lat = coordinates.lat;
// });
const rules = reactive<FormRules<RuleForm>>({
  landtype: [
    {
      required: true,
      message: '请选择地类',
      trigger: 'change',
    },
  ],
  lng: [
    {
      required: true,
      message: '请填写经度',
      trigger: 'change',
    },
  ],
  lat: [
    {
      required: true,
      message: '请填写纬度',
      trigger: 'change',
    },
  ],

  rank: [
    {
      required: true,
      message: '请选择等级',
      trigger: 'change',
    },
  ],
  date1: [
    {
      type: 'date',
      required: true,
      message: '请选择日期',
      trigger: 'change',
    },
  ],
  date2: [
    {
      type: 'date',
      required: true,
      message: '请选择日期',
      trigger: 'change',
    },
  ],
  explain: [
    {
      required: true,
      message: '请填写预警说明',
      trigger: 'change',
    },
  ],
});
const handleOpen = () => {
  console.log('handleOpen');
};
const addWarnPointIcon = async () => {
  console.log('addWarnPointIcon');
  var vectorLayer = new VectorLayer({
    source: vectorSource,
  });

  var pointFeature = new Feature({
    geometry: new Point([lngForm.value, latForm.value]),
  });
  // console.log(warnrank.value);
  let iconUrl = '';
  if (warningForm.rank == '普通预警') {
    iconUrl = '/public/tools/普通预警.png';
  }
  if (warningForm.rank == '高级预警') {
    iconUrl = '/public/tools/高级预警.png';
  }

  pointFeature.setStyle(
    new Style({
      image: new Icon({
        src: iconUrl,
        scale: 0.5, // 调整图标的大小
      }),
    }),
  );
  vectorSource.addFeature(pointFeature);
  map.value.addLayer(vectorLayer);
};
const submitForm = async (formEl: FormInstance | undefined) => {
  // console.log(warningForm);
  // console.log(lng);

  // console.log(warningForm.lng);
  if (!formEl) return;
  await formEl.validate((valid, fields) => {
    if (valid) {
      addWarnPoints();
      addWarnPointIcon();

      isShowWarningForm.value = false;
      // isWarningFormOpen.value = false;
      // warnrank.value = warningForm.rank;
      console.log('submit!');
    } else {
      console.log('error submit!', fields);
    }
  });
};
// 封装添加或编辑景区的接口
const addWarnPoints = async () => {
  // console.log(warningForm.date1);
  // console.log(warningForm.date2);
  const result = await addPointApi(warningForm);
  // console.log(result);
  //todo 后端结果类还没有封装,目前是void
  if (result.code == 200) {
    // dialogFormVisible.value = false;
    ElMessageBox({
      type: 'success',
      message: '添加成功',
    });
  } else {
    ElMessageBox({
      type: 'error',
      message: '添加失败',
    });
  }
};
const resetForm = (formEl: FormInstance | undefined) => {
  if (!formEl) return;
  formEl.resetFields();
};

const options = Array.from({ length: 10000 }).map((_, idx) => ({
  value: `${idx + 1}`,
  label: `${idx + 1}`,
}));
const handleClose = () => {
  console.log('handleClose');
  // isWarningFormOpen.value = false;

  // warnrank.value = '测试等级';
  // addWarnPoint();
  // console.log();
  // console.log('handleClose', isWarningFormOpen.value);
  //   props.isShow = false;
  //@ts-ignore
  isShowWarningForm.value = false;
};
</script>
<style lang="scss" scoped></style>
