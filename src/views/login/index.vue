<template>
  <div class="login_container">
    <el-row>
      <el-col :span="12" :xs="0"></el-col>
      <el-col :span="12" :xs="24">
        <el-card class="login_form">
          <h1>国土空间评价双规划系统</h1>
          <el-form :model="loginForm" ref="loginForms">
            <el-form-item prop="username">
              <el-input :prefix-icon="User" v-model="loginForm.username" clearable placeholder="Username" size="large"></el-input>
            </el-form-item>
            <el-form-item prop="password">
              <el-input type="password" :prefix-icon="Lock" show-password v-model="loginForm.password" size="large" placeholder="Password" clearable></el-input>
            </el-form-item>
            <!-- <el-form-item prop="verifyCode">
              <el-input :prefix-icon="Warning" show-password v-model="loginForm.verifyCode" placeholder="VerifyCode" size="large" maxlength="4"> </el-input>
            </el-form-item> -->
          </el-form>
          <el-form-item>
            <el-button :loading="loading" class="login_btn" type="primary" size="default" @click="login"> 登录 </el-button>
          </el-form-item>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>
<script setup lang="ts">
import { User, Lock, Warning } from '@element-plus/icons-vue';
import { Ref, computed, reactive, ref } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import { ElNotification } from 'element-plus';
// import { getTime } from '@/utils/time';
import useUserStore from '@/store/modules/user';
import { reqLogin, reqUserInfo } from '@/api/user/index.ts';
// import Identify from '@/components/VerifyCode/index.vue';
// VerifyCode import
let $router = useRouter();
// let $route = useRoute();
let loading = ref(false);

let useStore = useUserStore();
let loginForms = ref();

const loginForm = reactive({
  username: 'admin',
  password: '111111',
  // verifyCode: '1234',
});

const login = async () => {
  await loginForms.value.validate();
  loading.value = true;
  try {
    // reqLogin(loginForm)
    useStore.userLogin(loginForm);
    loading.value = false;
    ElNotification({
      type: 'success',
      message: '登陆成功',
      // title: `Hi, ${getTime()}好`,
    });
    // $router.push('/home');
    // console.log(loginForm);
    // console.log(await useStore.userLogin(loginForm));
    // await useStore.userLogin(loginForm);
    // let redirect: string = $route.query.redirect as string;
    // $router.push({ path: redirect || '/' });
    $router.push('/home');
  } catch (error) {
    loading.value = false;
    ElNotification({
      type: 'error',
      message: (error as Error).message,
    });
  }
};

// ../../assets/images/0050.png
</script>
<style lang="scss" scoped>
.login_container {
  width: 100%;
  height: 100vh;
  background: url('/bj2.jpg') no-repeat;
  background-size: cover;
  position: fixed;
  .login_form {
    position: relative;
    width: 55%;
    top: 25vh;
    left: 10vw;
    padding: 10px;
    background: transparent;
    h1 {
      background: linear-gradient(to right, blue, rgb(35, 60, 70));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      font-size: 40px;
      text-align: center;
      font-weight: 700;
      margin-bottom: 40px;
      margin-top: -10px;
    }
    .login_btn {
      width: 100%;
    }
  }
}
.el-card {
  box-shadow: rgba(0, 0, 0, 0.24) 0px 3px 8px;
}
:deep(.el-input-group__append, .el-input-group__prepend) {
  padding: 0;
}
</style>
