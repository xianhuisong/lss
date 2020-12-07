import job_commiters.bigquery_job as bigquery_job
import firestore_client as fc
import logging.config
import time

logging.config.fileConfig("logging.conf")
logger = logging.getLogger()


def job_execute(event, context):
    """
    triggered by pubsub, the event.id is the job id to be run
    Args:
        event: the input event
        context: cloud function runtime context

    Returns:

    """
    try:
        # get configurations from fire store
        import base64, json
        if 'data' in event:
            request_data = base64.b64decode(event['data']).decode('utf-8')

            json_data = json.loads(request_data)
            logger.info("request data : " + str(json_data))
            job_id = json_data['job_id'] if json_data['job_id'] else None
            if job_id:
                logger.info("execute job: " + str(job_id))
                job_config = fc.get_job(job_id)
                if job_config:
                    job_type = job_config['job_type']
                    if job_type == "BigQuery":
                        gcp_job_id = bigquery_job.submit_job(job_config)
                    else:
                        pass
                    log_msg = "job started : " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    job_log = {
                        u'job_id': job_id,
                        u'gcp_job_id': gcp_job_id,
                        u'log_msg': log_msg,
                        u'status': u'start',
                        u'start_time': start_time
                    }
                    fc.insert_log(job_log)
                else:
                    logger.error("Job Id not found: " + job_id)
            else:
                logger.info("job id not found in event" + str(event))
    except Exception as ex:
        print(ex)

if __name__ == '__main__':
    pass
    # event = {
    #     "job_id": u'1001'
    # }
    # job_execute(event, None)
