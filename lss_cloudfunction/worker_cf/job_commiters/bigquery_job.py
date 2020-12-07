import logging
import time
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition

logger = logging.getLogger()


def submit_job(job_config={}):
    logger.info("accept job :" + str(job_config))

    client = bigquery.Client()
    write_config = job_config['properties']['write_disposition']

    if write_config == "WRITE_APPEND":
        write_deposition = WriteDisposition.WRITE_APPEND
    elif write_config == "WRITE_TRUNCATE":
        write_deposition = WriteDisposition.WRITE_TRUNCATE
    elif write_config == "WRITE_EMPTY":
        write_deposition = WriteDisposition.WRITE_EMPTY
    else:
        logger.info("invalid value of write deposition")
        return
    query_job = client.query(
        job_config['properties']['query'],
        # Explicitly force job execution to be routed to a specific processing
        # location.
        # Specify a job configuration to set optional job resource properties.
        job_config=bigquery.QueryJobConfig(
            labels={"lss-demo": "big_query_job"}
            , write_disposition=write_deposition,
            destination=job_config['properties']['destination']
        ),
        # The client libraries automatically generate a job ID. Override the
        # generated ID with either the job_id_prefix or job_id parameters.
        # job_id_prefix="lss_demo_",
    )  # Make an API request.
    logger.info("Details for job {} :".format(query_job.job_id))
    # time.sleep(3)
    query_job = client.get_job(query_job.job_id)
    logger.info(
        "\tType: {}\n\tState: {}\n\tCreated: {}\n\tErrors: {}\n\tErrorResult: {}".format(
            query_job.job_type, query_job.state, query_job.created, query_job.errors, query_job.error_result
        )
    )
    return query_job.job_id
    # while True:
    #     query_job = client.get_job(query_job.job_id)
    #     print(
    #         "\tType: {}\n\tState: {}\n\tCreated: {}\n\tErrors: {}\n\tErrorResult: {}".format(
    #             query_job.job_type, query_job.state, query_job.created, query_job.errors, query_job.error_result
    #         )
    #     )
    #     if query_job.state == "DONE":
    #         break
    #     else:
    #         time.sleep(1)
    # print(query_job)


if __name__ == "__main__":
    pass
