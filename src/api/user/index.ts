import request from '@/utils/request'
import { loginForm, loginResponseForm, userResponseData } from './type'

enum API {
  LOGIN_URL = '/user/login',
  USERINFO_URL = '/user/info',
}
//登录接口方法
export const reqLogin = (data: loginForm) => {
  return request.post<loginResponseForm>(API.LOGIN_URL, data);
};
//获取用户信息接口方法
export const reqUserInfo = () =>
  request.get<any, userResponseData>(API.USERINFO_URL)
