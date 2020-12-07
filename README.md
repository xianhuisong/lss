# lss
Fire Store Schema:

job_log:
{
	job_id:int,
	gcp_job_id :string,
	start_time:datetime,
	end_time:datetime,
	status:string,  -- start, end, 
                  log_msg:string	
}
job_info:
{
	job_id:int,
	group_id: int,
	input:string,
	output:string,
	job_name:string,
	job_type:string
	schedule:string
	properties:json
	successor_id:
}


Architecture:
