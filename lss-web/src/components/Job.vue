<template>
  <div>
    <el-header style="background-color: white; float: right">
      <el-button type="primary" style="float: right">Add Job</el-button>
    </el-header>
    <div style="height: 20px"></div>
    <el-main>
      <el-table :data="tableData" style="white-space: pre-line">
        <el-table-column prop="job_id" label="Id" width="100">
        </el-table-column>
        <el-table-column
          prop="job_name"
          label="Name"
          width="150"
        ></el-table-column>
        <el-table-column prop="job_type" label="Type" width="80">
        </el-table-column>
        <el-table-column prop="next_job" label="NextJob" width="80`">
        </el-table-column>
        <el-table-column prop="Schedule" label="Schedule" width="100">
        </el-table-column>
        <el-table-column
          prop="properties"
          label="Properties"
          width="400"
          style="white-space: pre-line;"
        ></el-table-column>
        <el-table-column prop="Operation" label="Operation">
          <template v-slot="job">
            <el-button
              type="info"
              icon="el-icon-search"
              circle
              size="mini"
            ></el-button>
            <el-button
              type="primary"
              icon="el-icon-edit"
              circle
              size="mini"
            ></el-button>
            <el-button
              type="danger"
              icon="el-icon-delete"
              circle
              size="mini"
            ></el-button>
            <el-button
              type="success"
              icon="el-icon-video-play"
              circle
              size="mini"
              @click="runJob(job)"
            ></el-button>
          </template>
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
    runJob(job) {
      console.log(job.row.job_id);
      this.$confirm("Continue to run job : " + job.row.job_id, "Confirm", {
        confirmButtonText: "Confirm",
        cancelButtonText: "Cancel",
        type: "info",
      })
        .then(() => {
          axios
            .post("https://us-central1-cf-fs-project.cloudfunctions.net/lss_admin_cf", {
              operation: "run_job",
              job_id: job.row.job_id,
            })
            .then((response) => {
              console.log(response);
              this.$message({
                type: "success",
                message: "submit successfully!",
              });
            })
            .catch(function (error) {
              console.log(error);
            });
        })
        .catch(() => {
          this.$message({
            type: "info",
            message: "cancel done!",
          });
        });
    },
    getJob() {
      console.log("job query");
      axios
        .post(
            // "http://localhost:8081",
            "https://us-central1-cf-fs-project.cloudfunctions.net/lss_admin_cf",
             { operation: "query_job", })
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
