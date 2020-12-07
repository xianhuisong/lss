import time

import firestore_client as fc
from google.cloud import pubsub_v1


def job_complete(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
        Args:
             event (dict):  The dictionary with data specific to this type of
             event. The `data` field contains the PubsubMessage message. The
             `attributes` field will contain custom attributes if there are any.
             context (google.cloud.functions.Context): The Cloud Functions event
             metadata. The `event_id` field contains the Pub/Sub message ID. The
             `timestamp` field contains the publish time.
        """
    try:
        import base64, json
        if 'data' in event:
            job_log = base64.b64decode(event['data']).decode('utf-8')
        print('decode data {}'.format(job_log))
        job_log = json.loads(job_log)
        print('json data: ' + str(job_log))
        if job_log['protoPayload']:
            _payload = job_log['protoPayload']
            if _payload['methodName']:
                _methodName = _payload['methodName']
                if _methodName != "jobservice.jobcompleted":
                    print("Receive message: " + _methodName + "! Not Process")
                    return
                else:
                    print("Receive message: " + _methodName + "! Will Process")
                    _gcpJob = _payload['serviceData']['jobCompletedEvent']['job']
                    _gcpJobId = _gcpJob['jobName']['jobId']
                    _jobStatistic = _gcpJob['jobStatistics']
                    _endTime = _jobStatistic['endTime']
                    print("_gcpJob: " + str(_gcpJob))
                    print("_gcpJobId: " + str(_gcpJobId))
                    print("_jobStatistic: " + str(_jobStatistic))
                    print("_endTime: " + str(_endTime))
                    print("start to get firestore job log: " + str(_gcpJobId))
                    doc_id, doc = fc.get_log(_gcpJobId)
                    print("get document id: " + doc_id)
                    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    status = "Done"
                    log_msg = doc['log_msg'] + '\n' + 'job ended : ' + time.strftime("%Y-%m-%d %H:%M:%S",
                                                                                      time.localtime())
                    update_doc = {
                        "end_time": end_time,
                        "status": status,
                        "job_statics": _jobStatistic,
                        "log_msg": log_msg
                    }
                    res = fc.update_log(doc_id, update_doc)
                    print("Update document success: " + str(res))

                    # if there is successor jobs , then publish the job id into the pubsub and trigger the job
                    next_job_id = _next_job(doc['job_id'])
                    print("post process successfully! job id : " + str(next_job_id))
    except Exception as ex:
        print(ex)


def _next_job(job_id):
    next_job_id = None
    if job_id:
        job_info = fc.get_job(job_id)
        if job_info and job_info['next_job'] and str(job_info['next_job']).strip() != "":
            next_job_id = str(job_info['next_job']).strip()
            print("publish next job id: " + next_job_id)
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path('cf-fs-project', 'lss_job_trigger_topic')
            data = '{"job_id": "' + str(next_job_id) + '"}'
            # Data must be a bytestring
            data = data.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data)
            print("successfully publish next job id, message id : " + str(future.result()))
    return next_job_id


if __name__ == '__main__':
    pass
    # res = _next_job(u'1001')
    # print(res)
