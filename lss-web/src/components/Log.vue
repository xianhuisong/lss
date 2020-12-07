<template>
  <div>
    <div style="height: 20px"></div>
    <el-main>
      <el-table :data="tableData" style="white-space: pre-line">
        <el-table-column prop="job_id" label="Id" width="100">
        </el-table-column>
        <el-table-column
          prop="start_time"
          label="StartTime"
          width="200"
        ></el-table-column>
        <el-table-column prop="end_time" label="EndTime" width="200">
        </el-table-column>
        <el-table-column prop="status" label="Status" width="100`">
        </el-table-column>
        <el-table-column prop="log_msg" label="Logs" width="500" style="white-space: pre-line">
        </el-table-column>
      </el-table>
    </el-main>
  </div>
</template>

<script>
import axios from "axios";
export default {
  data() {
    return {
      tableData: [],
    };
  },
  created() {
    this.tableData = [];
  },
  mounted: function () {
    this.getJob();
  },
  methods: {
    getJob() {
      console.log("job query");
      axios
        .post(
          "https://us-central1-cf-fs-project.cloudfunctions.net/lss_admin_cf",
          // "http://localhost:8081",
          {
            operation: "query_log",
          }
        )
        .then((response) => {
          console.log(response);
          console.log(response.data);
          this.tableData = response.data.data;
        })
        .catch(function (error) {
          console.log(error);
        });
    },
  },
};
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
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
