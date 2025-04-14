//创建用户相关小仓库
import { defineStore } from 'pinia';
//引入接口
import { reqLogin, reqUserInfo } from '@/api/user';
//引入数据类型
import type { loginForm, loginResponseForm } from '@/api/user/type';
import type { UserState } from './types/type';
//引入操作本地存储的工具方法
import { SET_TOKEN, GET_TOKEN, REMOVE_TOKEN } from '@/utils/token';
import { constantRoute } from '@/router/routes';
let userUserStore = defineStore('User', {
  state: (): UserState => {
    return {
      //用户唯一标识token
      token: GET_TOKEN(),
      menuRoutes: constantRoute,
      username: '',
      avatar: '',
    };
  },
  //处理异步|逻辑的地方
  actions: {
    async userLogin(data: loginForm) {
      let result: loginResponseForm = await reqLogin(data);

      if (result.code == 200) {
        console.log(result.data.token);
        //本地存储持久化存储TOKEN
        SET_TOKEN(result.data.token as string);
        // console.log(result.data.token)
        //pinia仓库存储token
        this.token = result.data.token as string;

        // console.log(result.data.token)
        return 'ok';
      } else {
        return Promise.reject(new Error(result.data.message));
      }
    },
    async userInfo() {
      let result = await reqUserInfo();
      // console.log(result)
      if (result.code == 200) {
        this.username = result.data.username;
        console.log(this.username);
        this.avatar = result.data.avatar;
        return 'ok';
      } else {
        return Promise.reject('获取用户信息失败');
      }
    },
    userLogout() {
      this.token = '';
      this.username = '';
      this.avatar = '';
      // localStorage.removeItem('TOKEN')
      REMOVE_TOKEN();
    },
  },
  getters: {},
});
export default userUserStore;
