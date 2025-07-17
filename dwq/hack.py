import json
import logging

from prometheus_client import Counter
from pydisque.client import Client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FALLBACK_CONTROL_QUEUE = "control::123456789"


class FallbackDisque(object):
    def __init__(self, fallback_disque_url):
        self.disque = Client([fallback_disque_url])

    def connect(self):
        self.disque.connect()

    def connected(self):
        if not self.disque:
            return False
        if not self.disque.connected_node:
            return False

        return True


def enqueue_to_fallback_worker(job, fallback_disque):
    body = job.body
    body["original_control_queues"] = body["control_queues"]
    body["original_id"] = job.job_id
    body["control_queues"] = [FALLBACK_CONTROL_QUEUE]

    json_body = json.dumps(body)
    return fallback_disque.disque.add_job("default", json_body).decode("ascii")


def forward_from_fallback_worker(fallback_disque, worker_name, working_set, disque):
    _jobs = fallback_disque.disque.get_job(
        [FALLBACK_CONTROL_QUEUE])

    logger.info(f"received {len(_jobs)} from fallback worker")

    jobs = []
    for queue_name, job_id, json_body in _jobs:
        queue_name = queue_name.decode("ascii")
        job_id = job_id.decode("ascii")
        body = json.loads(json_body.decode("utf-8"))
        fallback_disque.disque.fast_ack(job_id)
        jobs.append(body)

    for job in jobs:
        result = job.get("result")

        if not result:
            logger.warning(
                f"Job {json.dumps(job)} did not contain a result not forwarding...")

            continue

        original_control_queues = result.get(
            "body", {}).get("original_control_queues")

        if not original_control_queues:
            logger.warning(
                f"Job {json.dumps(job)} did not contain a 'original_control_queue' field, not forwarding...")
            continue


        del result["body"]["original_control_queues"]

        original_job_id = result["body"]["original_id"]
        del result["body"]["original_id"]

        result["worker"] = worker_name

        for queue in original_control_queues:
            if queue == "$jobid":
                queue = original_job_id
            disque.add_job(
                queue,
                json.dumps({
                    "job_id": original_job_id,
                    "state": "done",
                    "result": result
                })
            )

        disque.ack_job(original_job_id)
        working_set.discard(original_job_id)
