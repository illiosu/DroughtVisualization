import axios from 'axios';
import { ElMessage } from 'element-plus';
import { SET_TOKEN, GET_TOKEN, REMOVE_TOKEN } from '@/utils/token';

//创建axios实例
let request = axios.create({
  baseURL: import.meta.env.VITE_APP_BASE_API,
  timeout: 5000,
});
//请求拦截器
request.interceptors.request.use((config) => {
  const token = GET_TOKEN(); // 通常像 "Bearer your_token"
  config.headers.Authorization = token;
  // console.log('请求拦截器')
  return config;
});
//响应拦截器
request.interceptors.response.use(
  (response) => {
    console.log('响应拦截器')
    return response.data;
  },
  (error) => {
    //处理网络错误
    let msg = '';
    let status = error.response.status;
    switch (status) {
      case 401:
        msg = 'token过期';
        break;
      case 403:
        msg = '无权访问';
        break;
      case 404:
        msg = '请求地址错误';
        break;
      case 500:
        msg = '服务器出现问题';
        break;
      default:
        msg = '无网络';
    }
    ElMessage({
      type: 'error',
      message: msg,
    });
    return Promise.reject(error);
  },
);
export default request;
