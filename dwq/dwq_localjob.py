#!/usr/bin/env python3

import json
import random
import os
import signal
import sys
import time
import argparse
import subprocess
import shlex

from dwq import Job, Disque
import dwq.util as util

from dwq.version import __version__


def sigterm_handler(signal, stack_frame):
    raise SystemExit()


def parse_args():
    parser = argparse.ArgumentParser(
        prog="dwq-localjob", description="dwq: disque-based work queue"
    )

    parser.add_argument(
        "-D",
        "--disque-url",
        help="specify disque instance [default: localhost:7711]",
        type=str,
        action="store",
        default=os.environ.get("DWQ_DISQUE_URL", "localhost:7711"),
    )

    parser.add_argument(
        "-v", "--verbose", help="enable status output", action="store_true"
    )

    parser.add_argument(
        "--version", action="version", version="%(prog)s " + __version__
    )

    parser.add_argument("command", type=str, nargs="+")

    parser.add_argument("-c", "--command-override", type=str)

    return parser.parse_args()


def vprint(*args, **kwargs):
    global verbose
    if verbose:
        print(*args, **kwargs)


verbose = False


def main():
    global verbose
    args = parse_args()

    try:
        control_queue = os.environ["DWQ_CONTROL_QUEUE"]
    except KeyError:
        print("dwqc: error: DWQ_CONTROL_QUEUE unset.")
        sys.exit(1)

    try:
        parent_jobid = os.environ["DWQ_JOBID"]
    except KeyError:
        print("dwqc: error: DWQ_JOBID unset.")
        sys.exit(1)

    original_control_queues_string = os.environ.get("ORIGINAL_CONTROL_QUEUES")
    original_id = os.environ.get("ORIGINAL_ID")

    verbose = args.verbose
    signal.signal(signal.SIGTERM, sigterm_handler)
    Disque.connect([args.disque_url])

    unique = random.random()

    job_command = shlex.join(args.command)

    before = time.time()
    result = subprocess.run(job_command, shell=True, capture_output=True)
    cmd_runtime = time.time() - before
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    result_body = {
        "status": result.returncode,
        "output": stdout + stderr,
        "worker": os.environ.get("DWQ_WORKER"),
        "body": {
            "command": args.command_override or job_command,
        },
        "unique": str(unique),
        "times": {
            "cmd_runtime": cmd_runtime,
        },
    }

    # let controller know there's a job result incoming
    job_id = "localjob-" + str(unique)
    disque = Disque.get()
    body = {
        "parent": parent_jobid,
        "subjob": job_id,
        "unique": os.environ.get("DWQ_JOB_UNIQUE"),
    }

    if original_control_queues_string:
        original_control_queues = original_control_queues_string.split(" ")

        body["original_control_queues"] = original_control_queues
        result_body["body"]["original_control_queues"] = original_control_queues

        result_body["is_subjob"] = True

    if original_id:
        body["original_id"] = original_id

    Job.add(control_queue, body, None)

    # send actual job result
    disque.add_job(
        control_queue,
        json.dumps({"job_id": job_id, "state": "done", "result": result_body}),
    )
