import Vue from 'vue'
import Router from 'vue-router'
import HelloWorld from '@/components/HelloWorld'
import Job from '@/components/Job'
import Log from '@/components/Log'
// import ElementUi from 'element-ui'
// import 'element-ui/lib/theme-chalk/index.css';
Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'HelloWorld',
      component: HelloWorld,
      children: [
        {
          path: "/Job",
          name: "Job",
          component: Job
        },
        {
          path: "/Log",
          name: "Log",
          component: Log

        }

      ]

    }
  ]
})
