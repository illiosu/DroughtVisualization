// 注册全局组件
import SvgIcon from './SvgIcon/index.vue';
const allGlobalComponent:any = {SvgIcon};
export default{
    install(app:any){
        Object.keys(allGlobalComponent).forEach(key=>{
            app.component(key,allGlobalComponent[key]);
        })
    }
}