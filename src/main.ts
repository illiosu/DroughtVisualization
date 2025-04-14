import { createApp } from 'vue'

import App from '@/App.vue'
const app = createApp(App)

//element-plus
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
//国际化
//@ts-ignore
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
app.use(ElementPlus, {
  locale: zhCn,
})

//svg插件配置代码
import 'virtual:svg-icons-register'

//引入模板的全局样式
import '@/styles/index.scss'

import router from './router'
app.use(router)

//pinia
import { createPinia } from 'pinia'
const pinia = createPinia()
app.use(pinia)

//这种注册不了，未知原因
// //全局组件
// import allGlobalComponent from '@/components/index'
// console.log(allGlobalComponent);

//注册全局icon
import * as Icons from '@element-plus/icons'
// 注册Icons 全局组件
Object.keys(Icons).forEach(key => {
  app.component(key, Icons[key])
})

//echarts
import * as echarts from 'echarts';//引入echarts
app.config.globalProperties.$echarts = echarts;//挂载到全局



//将应用挂载到DOM中
app.mount('#app')