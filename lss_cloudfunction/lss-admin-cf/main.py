from google.cloud import firestore
import logging.config
from flask import Response, Request
from google.cloud import pubsub_v1

logging.config.fileConfig("logging.conf")
logger = logging.getLogger()

db = firestore.Client(project="cf-fs-project")


def run_job(job_id):
    print(job_id)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('cf-fs-project', 'lss_job_trigger_topic')
    data = '{"job_id": "' + job_id + '"}'
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print("successfully publish next job id, message id : " + str(future.result()))


def list_job_debug():
    response = []
    try:
        record1 = {
            "group_id": "01",
            "input": "cf-fs-project.lss_raw.user_info",
            "job_id": "1001",
            "job_name": "lss_demo_raw2stg",
            "job_type": "BigQuery",
            "next_job": "1002",
            "output": "cf-fs-project.lss_temp.user_info_temp",
            "properties": {
                "destination": "cf-fs-project.lss_temp.user_info_temp",
                "job_id_prefix": "lss_demo_",
                "query": "select id,name,age,gender from  cf-fs-project.lss_raw.user_info where gender='F'",
                "write_disposition": "WRITE_APPEND"
            }
        }
        record2 = {
            "group_id": "01",
            "input": "cf-fs-project.lss_temp.user_info_temp",
            "job_id": "1002",
            "job_name": "lss_demo_stg2inst",
            "job_type": "BigQuery",
            "next_job": "",
            "output": "cf-fs-project.lss_insight.user_info_insight",
            "properties": {
                "destination": "cf-fs-project.lss_insight.user_info_insight",
                "job_id_prefix": "lss_demo_",
                "query": "select id,name,age,gender, 'Dalian' as address from  cf-fs-project.lss_temp.user_info_temp",
                "write_disposition": "WRITE_APPEND"
            }
        }
        response.append(record2)
        response.append(record1)

    except Exception as ex:
        return ex
    return response


def list_job(offset, limit):
    response = []
    try:
        job_collection = db.collection(u'lss_jobs').stream()
        for job in job_collection:
            response.append(job.to_dict())
        print(response)
    except Exception as ex:
        return ex
    return response


def create_job(job={}):
    logger.info(job)
    try:
        if job['job_id'] and job['job_name'] and job['job_type'] and job['group_id']:
            job_ref = db.collection(u'lss_jobs').document(job['job_id'])
            job_ref.set(job, merge=True)
            logger.info("add job into firestore:" + str(job['job_id']))
        else:
            logger.info("required fields are missing")
    except Exception as e:
        logger.error("errors happen when insert document.")
        logger.error(e)


def list_logs(offset, limit):
    """
    get execution log from firestore by gcp job id ,eg: big query job id
    Args:
        gcp_job_id: the gcp job id , like bigquery job id

    Returns: the tuple (document id, document dict)

    """
    response = []
    try:
        job_collection = db.collection(u'lss_logs').stream()
        for job in job_collection:
            response.append(job.to_dict())
        print(response)
    except Exception as ex:
        return ex
    return response


def handler(request: Request):
    """Responds to any HTTP request.
       Args:
           request (flask.Request): HTTP request object.
       Returns:
           The response text or any set of values that can be turned into a
           Response object using
           `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
       """
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

        # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    code = 0
    message = "success"
    data = []
    print(request)
    request_json = request.get_json()
    if request_json and 'operation' in request_json:
        opt = request_json['operation']
        if opt == "query_job":
            data = list_job(0, 0)
            # data = list_job_debug()
            for index, item in enumerate(data):

                properties = item['properties']
                pro_str = ""
                for key, value in properties.items():
                    line = str(key) + " = " + str(value) + "\n"
                    pro_str += line
                item['properties'] = pro_str
                data[index] = item
        elif opt == "query_log":
            data = list_logs(0, 0)
        elif opt == "run_job":
            job_id = request_json['job_id']
            if job_id:
                run_job(job_id)
    else:
        code = -1
        message = 'invalid request!'
    result = {"code": code, "message": message, "data": data}
    return result, 200, headers


if __name__ == '__main__':
    pass
    # res = list_logs(0, 0)
    # print(res)
