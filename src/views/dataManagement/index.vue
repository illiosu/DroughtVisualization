<template>
  <div>
    <el-card class="box-card" style="margin: 10px">
      <!-- 添加按钮 -->
      <el-button type="primary" @click="addScenicSpot" icon="plus">添加敏感点</el-button>
      <!-- 表格 -->
      <el-table :data="warnPointsList" style="width: 100%; margin: 10px 0px" border>
        <!-- <el-table-column label="土地类型" width="80px" align="center" type="index"></el-table-column>
        <el-table-column label="经度">
          <template #="{ row, $index }">
            <pre style="color: black">{{ row.name }}</pre>
          </template>
        </el-table-column> -->
        <el-table-column label="土地类型" prop="土地类型"> </el-table-column>

        <el-table-column label="纬度" prop="经度"></el-table-column>
        <el-table-column label="时间粗略" prop="时间粗略"></el-table-column>

        <el-table-column label="时间精确" prop="时间精确"></el-table-column>

        <el-table-column label="级别" prop="级别"></el-table-column>
        <el-table-column label="说明" prop="说明"></el-table-column>

        <el-table-column label="操作" width="180px">
          <template #="{ row, $index }">
            <el-button type="primary" size="small" icon="Edit" @click="updateWarnPoint(row)">编辑</el-button>
            <el-button type="primary" size="small" icon="Delete" @click="deleteWarnPoointApi(row.id)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination
        v-model:current-page="pageNo"
        v-model:page-size="limit"
        :page-sizes="[2, 4, 6, 8]"
        :background="true"
        layout="prev, pager, next, jumper, ->,sizes,total"
        :total="total"
        @current-change="getWarnPoints"
        @size-change="getWarnPoints" />
    </el-card>
    <el-dialog v-model="dialogFormVisible" :title="form.id ? '编辑敏感点' : '添加敏感点'" width="500" @close="handleClose">
      <el-form style="width: 80%">
        <el-form-item label="土地类型" prop="name">
          <el-input v-model="form.landtype" placeholder="请输入土地类型"></el-input>
        </el-form-item>
        <el-form-item label="经度" prop="name">
          <el-input v-model="form.lng" placeholder="请输入经度"></el-input>
        </el-form-item>
        <el-form-item label="纬度" prop="name">
          <el-input v-model="form.lat" placeholder="请输入纬度"></el-input>
        </el-form-item>
        <el-form-item label="时间粗略" prop="name">
          <el-input v-model="form.date1" placeholder="请输入时间粗略"></el-input>
        </el-form-item>
        <el-form-item label="时间精确" prop="name">
          <el-input v-model="form.date2" placeholder="请输入时间精确"></el-input>
        </el-form-item>
        <el-form-item label="级别" prop="name">
          <el-input v-model="form.rank" placeholder="请输入级别"></el-input>
        </el-form-item>
        <el-form-item label="说明" prop="name">
          <el-input v-model="form.explain" placeholder="请输入说明"></el-input>
        </el-form-item>
      </el-form>
      <!-- 具名插槽 -->
      <template #footer>
        <el-button type="default" @click="cancel">取消</el-button>
        <el-button type="primary" @click="confirm"> 确定 </el-button>
      </template>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import { ref, onMounted, reactive } from 'vue';
import { reqGetPoints, reqGetPointsCount, addPointApi, reqDelPoint } from '@/api/test/index.ts';
import { ElMessageBox } from 'element-plus';
import WarningForm from '@/components/WarningForm/index.vue';
import { id } from 'element-plus/es/locales.mjs';
const dialogTableVisible = ref(false);
const dialogFormVisible = ref<boolean>(false);
const formLabelWidth = '140px';

const form = reactive({
  id: '',
  landtype: '',
  lng: '',
  lat: '',
  date1: '',
  date2: '',
  rank: '',
  explain: '',
});

// import { test } from '@/api/test/index.ts';
// 当前页码
let pageNo = ref<number>(1);
// 每页显示条数
let limit = ref<number>(2);
// 总条数
let total = ref<number>(0);
// 存储景区信息的数组
let warnPointsList = ref([]);

// 封装获取敏感点信息的接口
const getWarnPoints = async () => {
  const result = await reqGetPoints(pageNo.value, limit.value);
  if (result.code == 200) {
    console.log(result);
    warnPointsList.value = result.data;
  }
};
// 封装获取景区总数的接口
const getWarnPointsCount = async () => {
  const result = await reqGetPointsCount();
  if (result.code == 200) {
    // console.log(result.data);
    total.value = result.data;
  }

  // console.log(result);
};
// 封装添加或编辑景区的接口
const addOrUpdateWarnPointApi = async () => {
  console.log(form);
  const result = await addPointApi(form);
  console.log(result);
  //todo 后端结果类还没有封装,目前是void
  if (result.code == 200) {
    dialogFormVisible.value = false;
    ElMessageBox({
      type: 'success',
      message: '添加成功',
    });

    getWarnPoints();
    getWarnPointsCount();
  } else {
    ElMessageBox({
      type: 'error',
      message: '添加失败',
    });
  }
};
// 封装删除景区的接口
const deleteWarnPoointApi = async (id: number) => {
  console.log(id);
  ElMessageBox.confirm('确认删除吗？')
    .then(async () => {
      // console.log(id);
      // console.log('deleteScenicSpotApi')
      const result = await reqDelPoint(id);
      // console.log(result);
      //todo 后端结果类还没有封装,目前是void
      if (result.code == 200) {
        dialogFormVisible.value = false;
        ElMessageBox({
          type: 'success',
          message: '删除成功',
        });
        getWarnPoints();
        getWarnPointsCount();
      } else {
        ElMessageBox({
          type: 'error',
          message: '删除失败',
        });
      }
    })
    .catch(() => {
      console.log('cancel');
    });
};

// const getTest = async () => {
//   const data = await test();
//   console.log(data);
// };
onMounted(() => {
  getWarnPoints();
  getWarnPointsCount();
  //   getScenicSpot();
  //   getScenicSpotCount();
  // getTest();
});
const addScenicSpot = () => {
  dialogFormVisible.value = true;
};
const updateWarnPoint = (row) => {
  console.log(row.id);
  dialogFormVisible.value = true;

  // todo form.x 和 form.y 没有解决赋值问题
  // // ES6合并对象
  // Object.assign(form, row);
  console.log(row.土地类型);
  form.id = row.id;
  form.landtype = row.土地类型;
  form.lng = row.经度;
  form.lat = row.纬度;
  form.date1 = row.时间粗略;
  form.date2 = row.时间精确;
  form.rank = row.级别;
  form.explain = row.说明;
};
const cancel = () => {
  dialogFormVisible.value = false;
};
const confirm = () => {
  // dialogFormVisible.value = false;
  ElMessageBox.confirm('确认提交吗？')
    .then(() => {
      addOrUpdateWarnPointApi();
      dialogFormVisible.value = false;
      // 再次请求接口获取数据
      getWarnPoints();
      // 清空表单
      form.id = '';
      form.landtype = '';
      form.lng = '';
      form.lat = '';
      form.date1 = '';
      form.date2 = '';
      form.rank = '';
      form.explain = '';

      console.log('submit');
    })
    .catch(() => {
      console.log('cancel');
    });
};
//dialog关闭时的回调函数
const handleClose = () => {
  // dialogFormVisible.value = false;
  console.log('handleClose');
  // 清空表单
  form.id = '';
  form.landtype = '';
  form.lng = '';
  form.lat = '';
  form.date1 = '';
  form.date2 = '';
  form.rank = '';
  form.explain = '';
};
// // 分页器页码发生变化时触发
// const changePageNo = (val: number) => {
//   // pageNo.value = val;
//   getScenicSpot();
// };
</script>
<style lang="scss" scoped></style>
