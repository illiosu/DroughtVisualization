// import { iconPropType } from 'element-plus/es/utils/index.js'

export const constantRoute = [
  {
    path: '/',
    component: () => import('@/views/login/index.vue'),
    name: 'login',
    meta: {
      title: '登陆',
      hidden: true,
      icon: 'CircleCloseFilled',
    },
  },

  {
    path: '/home',
    component: () => import('@/layout/index.vue'),
    name: 'layout',
    meta: {
      title: '',
      hidden: false,
      icon: '',
    },
    redirect: '/home',
    children: [
      {
        path: '/home',
        component: () => import('@/views/home/index.vue'),
        meta: {
          title: '地图浏览与操作',
          hidden: false,
          icon: 'HomeFilled',
        },
      },
    ],
  },

  {
    path: '/404',
    component: () => import('@/views/404/index.vue'),
    meta: {
      title: '404',
      hidden: true,
      icon: 'CircleCloseFilled',
    },
  },
  {
    path: '/data',
    component: () => import('@/views/dataManagement/index.vue'),
    meta: {
      title: '数据管理',
      hidden: true,
      icon: 'CircleCloseFilled',
    },
  },
  {
    path: '/sd',
    component: () => import('@/layout/index.vue'),
    meta: {
      title: '',
      hidden: false,
      icon: 'CircleCloseFilled',
    },
    redirect: '/spatiotemporaldynamic',
    children: [
      {
        path: '/spatiotemporaldynamic',
        component: () => import('@/views/spatiotemporaldynamic/index.vue'),
        meta: {
          title: '时空动态分析',
          hidden: false,
          icon: 'DataLine',
        },
      },
    ],
  },
  {
    path: '/mf',
    component: () => import('@/layout/index.vue'),
    meta: {
      title: '',
      hidden: false,
      icon: 'CircleCloseFilled',
    },
    redirect: '/multifactor',
    children: [
      {
        path: '/multifactor',
        component: () => import('@/views/multifactor/index.vue'),
        meta: {
          title: '多因素综合评价分析',
          hidden: false,
          icon: 'Orange',
        },
      },
    ],
  },

  // {
  //   path: '/warning',
  //   component: () => import('@/layout/index.vue'),
  //   meta: {
  //     title: '',
  //     hidden: false,
  //     icon: 'CircleCloseFilled',
  //   },
  //   redirect: '/warning',
  //   children: [
  //     {
  //       path: '/warning',
  //       component: () => import('@/views/warning/index.vue'),
  //       meta: {
  //         title: '多因素综合评价分析',
  //         hidden: false,
  //         icon: 'Orange',
  //       },

  //     },
  //   ],
  // },

    {
      path: '/warning',
      component: () => import('@/layout/index.vue'),
      name: 'Pruduct',
      meta: {
        title: '风险预警',
        icon: 'Warning',
        hidden: false,
      },
      redirect:'/warning/addpoints',
      children: [
        {
          path: '/warning/addpoints',
          component: () => import('@/views/warning/addpoints/index.vue'),
          name: 'Trademark',
          meta: {
            title: '敏感点预警',
            icon: 'StarFilled',
          },
     
        },
        
        {
          path: '/warning/getpoints',
          component: () => import('@/views/warning/getpoints/index.vue'),
          name: 'Attr',
          meta: {
            title: '预警点位一览',
            icon: 'Document',
          },
        },
      ],
    },
  {
    path: '/:pathMatch(.*)*',
    redirect: '/404',
    name: 'Any',
    meta: {
      title: '任意路由',
      hidden: true,
      icon: 'Operation',
    },
  },
];
