import datetime
from google.cloud import firestore
import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger()

db = firestore.Client(project="cf-fs-project")


def get_log(gcp_job_id):
    """
    get execution log from firestore by gcp job id ,eg: big query job id
    Args:
        gcp_job_id: the gcp job id , like bigquery job id

    Returns: the tuple (document id, document dict)

    """
    logger.info(gcp_job_id)
    try:
        logs_ref = db.collection(u'lss_logs')
        # use stream instead of get function
        docs = logs_ref.where(u'gcp_job_id', u'==', gcp_job_id).limit(1).get()
        if len(docs) > 0:
            # only one documents returned because the query limitation`
            for doc in docs:
                logger.info(u' doc {} => {}'.format(doc.id, doc.to_dict()))
        return doc.id, doc.to_dict()
    except Exception as ex:
        logger.error(ex)


def update_log(log_id, doc):
    """
    Update job log document: endTime, logs
    Args:
        log_id: the job log document id
        doc:the attributes need to be updated in the document

    Returns:0: success , -1: failed, message

    """
    try:
        code = 0
        message = "success"
        if log_id and doc:
            log_ref = db.collection(u'lss_logs').document(log_id)
            if log_ref.get().exists:
                logger.info("Update document by id:{} , value:{} ".format(log_id, doc))
                log_ref.update(
                    doc
                )
            else:
                code = -1
                message = "document not found: " + str(log_id)
                logger.error("document not found, type: {}, id: {}".format("lss_logs", id))
        return code, message
    except Exception as ex:
        logger.error("Error happens: ")
        logger.error(ex)
        return -1, "Errors" + str(ex)


def insert_log(log={}):
    logger.info(log)
    try:
        if log['job_id'] and log['gcp_job_id']:
            job_ref = db.collection(u'lss_logs').document()
            log['status'] = 'start'
            log['start_time'] = datetime.datetime.now()
            job_ref.set(log, merge=True)
            logger.info("add logs into firestore:")
            logger.info(job_ref.get().to_dict())
        else:
            logger.info("required fields are missing")
    except Exception as e:
        logger.error("errors happen when insert document.")
        logger.error(e)


def get_job(job_id=None):
    if id:
        try:
            doc_ref = db.collection(u'lss_jobs').document(job_id)
            doc = doc_ref.get()
            if doc.exists:
                logger.info(f'Document data: {doc.to_dict()}')
                return doc.to_dict()
            else:
                logger.info(u'No such document!')
        except Exception as e:
            logger.error(e)


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


if __name__ == '__main__':
    pass
    # job_properties = {
    #     u'query': u"select id,name,age,gender ,'Dalian' as address from  cf-fs-project.lss_raw.user_info",
    #     u'write_disposition': u'WRITE_APPEND',
    #     u'destination': u'cf-fs-project.lss_insight.user_info_inst',
    #     u'job_id_prefix': u'lss_demo_'
    # }
    # job_add = {
    #     u'job_id': u'1001',
    #     u'group_id': u'01',
    #     u'job_name': u'lss_demo_raw2insight',
    #     u'job_type': u'BigQuery',
    #     u'input': u'cf-fs-project.lss_raw.user_info',
    #     u'output': u'cf-fs-project.lss_insight.user_info_inst',
    #     u'properties': job_properties
    # }
    # create_job(job_add)
    # res = get_job(u'1001')
    # logger.info(res)
    # job_log = {
    #     u'job_id': u'1001',
    #     u'gcp_job_id': u'test',
    #     u'log_msg': u'job started',
    # }
    #
    # insert_log(job_log)
    # gcp_log_id = 'lss_demo_65563df0-837e-44de-b9fb-d6b57b7fdf95'
    # lss_log = get_log(gcp_log_id)
    # print(lss_log)
    # ('D8fCt9kejyzF5l9HFOGn',
    #  {'status': 'start', 'gcp_job_id': 'lss_demo_65563df0-837e-44de-b9fb-d6b57b7fdf95', 'log_msg': 'job started',
    #   'job_id': '1001', 'start_time': DatetimeWithNanoseconds(2020, 11, 20, 17, 56, 49, 825796, tzinfo= < UTC >)})
    # document_id = 'D8fCt9kejyzF5l9HFOGn'
    # job_statics = {
    #     "billingTier": 1,
    #     "createTime": "2020-11-20T09:56:46.047Z",
    #     "endTime": "2020-11-20T09:56:48.465Z",
    #     "queryOutputRowCount": "4",
    #     "referencedTables": [
    #         {
    #             "datasetId": "lss_raw",
    #             "projectId": "cf-fs-project",
    #             "tableId": "user_info"
    #         }
    #     ],
    #     "startTime": "2020-11-20T09:56:46.356Z",
    #     "totalBilledBytes": "10485760",
    #     "totalProcessedBytes": "100",
    #     "totalSlotMs": "8404",
    #     "totalTablesProcessed": 1
    # }
    # end_time = job_statics['endTime']
    # status = "Done"
    # update_doc = {
    #     "end_time": end_time,
    #     "status": status,
    #     "job_statics": job_statics
    # }
    # res = update_log(document_id, update_doc)
    # print(res)
