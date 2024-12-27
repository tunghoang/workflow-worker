from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from rq.command import send_stop_job_command
from argparse import ArgumentParser
import os

from constants import DEPENDENCY_GRAPH

REDIS_HOST=os.getenv("REDIS_HOST") or "192.168.0.91"
#REDIS_PASS="apom123"
REDIS_PASS=os.getenv("REDIS_PASS")


__redis_conn = None
__queue = None
__secondaryQueue = None
def __getRedis():
  global __redis_conn
  if __redis_conn is None:
    __redis_conn = Redis(host=REDIS_HOST, password=REDIS_PASS)
  return __redis_conn
def __getSecondaryQueue():
  global __secondaryQueue
  if __secondaryQueue is None:
    __secondaryQueue = Queue("secondary", connection=__getRedis())
  return __secondaryQueue

def __getQueue():
  global __queue
  if __queue is None:
    __queue = Queue(connection=__getRedis())
  return __queue
def enqueueJob(job_path, start, job_timeout='12h', secondaryQueue=False):
  queue = __getQueue() if not secondaryQueue else __getSecondaryQueue()
  job = queue.enqueue(job_path, job_timeout=job_timeout, args=(start, ))
  print(job)

def enqueueNextJobs(pipeline, stageName, start):
  nextJobs = DEPENDENCY_GRAPH[pipeline][stageName]['nextJobs']
  for j in nextJobs:
    #print(f"Enqueue job: {j}")
    enqueueJob(f'{j["pipeline"]}.tasks.{j["stage"]}', start, secondaryQueue=True if j.get('queue') == 'secondary' else False)

def getWorkers():
  workers = Worker.all(queue=__getQueue())
  result = []
  for w in workers:
    name = f'[P] {w.hostname}({w.pid})'
    status = w.state
    job = {'description': 'None'}
    if w.state == 'busy':
      try:
        job = w.get_current_job().to_dict()
      except:
        job['description'] = "Unknown"
    result.append({'name': name, 'status': status, 'job': job['description']})
  workers = Worker.all(queue=__getSecondaryQueue())
  for w in workers:
    name = f'[S] {w.hostname}({w.pid})'
    status = w.state
    job = {'description': 'None'}
    if w.state == 'busy':
      try:
        job = w.get_current_job().to_dict()
      except:
        job['description'] = "Unknown"
    result.append({'name': name, 'status': status, 'job': job['description']})

  print(result);
  return result

def getWorkers1():
  return Worker.all(queue=__getQueue())
if __name__ == "__main__":
  parser = ArgumentParser(description="master process")
  parser.add_argument('-j', '--job', dest="job_path", required=True, help="Job path in dot notation")
  parser.add_argument('-s', '--start', dest="start", required=True, help="start date")
  parser.add_argument('-e', '--enqueue', action="store_true", help="enqueue or not")
  parser.add_argument('-f', '--fetch', action="store_true", help="fetch")
  parser.add_argument('-S', '--secondaryQueue', action="store_true", help="secondary queue or not")
  parser.add_argument('--stop', action="store_true", help="stop")
  args = parser.parse_args()

  print(args)

  redis_conn = __getRedis()
  if args.enqueue:
    if args.secondaryQueue:
      enqueueJob(args.job_path, args.start, secondaryQueue=True)
    else:
      enqueueJob(args.job_path, args.start)
    #queue = Queue(connection=redis_conn)
    #job = queue.enqueue(args.job_path, job_timeout=12*3600, args=(args.start, ))
    #print(job)
  elif args.fetch:
    job = Job.fetch(args.job_path, connection=redis_conn)
    print(job)
  elif args.stop:
    send_stop_job_command(redis_conn, args.job_path)
