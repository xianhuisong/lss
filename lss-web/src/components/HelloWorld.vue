<template>
  <el-container style="height: 500px; border: 1px solid #eee">
    <el-aside width="200px" style="background-color: rgb(238, 241, 246)">
      <el-menu :default-active="$route.path" router>
        <el-submenu index="1">
          <template slot="title"
            ><i class="el-icon-message"></i>Job List</template
          >
          <el-menu-item-group>
            <el-menu-item index="/Job">Job List</el-menu-item>
          </el-menu-item-group>
        </el-submenu>
        <el-submenu index="2">
          <template slot="title"><i class="el-icon-menu"></i>Job Logs</template>
          <el-menu-item-group>
            <el-menu-item index="/Log" >Job Logs</el-menu-item>
          </el-menu-item-group>
        </el-submenu>
      </el-menu>
    </el-aside>
  <el-container>
      <el-main>
        <router-view/>
      </el-main>
  </el-container>
  </el-container>
</template>

<script>
export default {
  // data() {
  //   const item = {
  //     Id: "2016-05-02",
  //     Name: "王小虎",
  //     Type: "上海市普陀区金沙江路 1518 弄",
  //     Schedule: "Manually",
  //     Properties: "{query:'select * from test'}",
  //   };
  //   return {
  //     tableData: Array(5).fill(item),
  //   };
  // },
};
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style>
.el-header {
  background-color: #b3c0d1;
  color: #333;
  line-height: 60px;
}

.el-aside {
  color: #333;
}
.el-table .cell {
white-space: pre-line;
}
</style>
