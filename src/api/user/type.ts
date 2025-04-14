//登录接口需要携带参数ts对象
export interface loginForm {
    username: string
    password: string
  }
  interface dataType {
    token?: string,
    message?:string
  }
  //登录接口返回的数据类型
  export interface loginResponseForm {
    code: number
    data: dataType
  }
  interface user {
    userId: number
    avatar: string
    username: string
    password: string
    desc: string
    roles: string[]
    buttons: string[]
    routes: string[]
    token: string
  }
  //定义服务器返回用户信息相关的用户类型
  export interface userResponseData {
    code: number
    data: user
  }
  