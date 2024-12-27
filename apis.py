import traceback, os
import requests
import json

from constants import DEPENDENCY_GRAPH
#__URL = 'http://192.168.0.91:8000/state/'
__URL = os.getenv('SERVICE_URL') or 'http://192.168.0.91:8000/state/'
def get_pipeline_task(pipeline, stage, on_date):
  try:
    r = requests.put(__URL, 
      headers={"Content-Type": "application/json"}, 
      data=json.dumps({'pipeline': pipeline, 'stage': stage, 'start': str(on_date)}))
    print(r.text)
    return json.loads(r.text)
  except:
    traceback.print_exc()
    return None

def upsert_pipeline_task(pipeline, stage, on_date, status=0):
  try:
    r = requests.post(__URL, 
      headers={"Content-Type": "application/json"}, 
      data=json.dumps({'pipeline': pipeline, 'stage': stage, 'start': str(on_date), 'status': status}))
    print(r.text)
    return json.loads(r.text)
  except:
    traceback.print_exc()
    return None

def delete_pipeline_tasks(pipeline, stage, on_date):
  r = requests.delete(__URL, 
    headers={"Content-Type": "application/json"}, 
    data=json.dumps({'pipeline': pipeline, 'stage': stage, 'start': str(on_date)}))
  print(r.text)

def can_run(pipeline, stageName, dependOn, startDate):
    stageTask = get_pipeline_task(pipeline, stageName, startDate)
    print(pipeline, stageName, stageTask, startDate)
    if len(stageTask) > 0 and int(stageTask[0]['status']) > 0:
        ## Should not rerun
        return False, "Already run"
    elif len(stageTask) > 0 and stageTask[0]['status'] == 0:
        return False, "It is running"

    if dependOn is None:
        return True, None

    dependOnTask = get_pipeline_task(pipeline, dependOn, startDate)
    if len(dependOnTask) and dependOnTask[0]['status'] > 0:
        return True, None
    else:
        return False, f"{dependOn} is not completed"

def can_run1(pipeline, stageName, dependJob, startDate):
    stageTask = get_pipeline_task(pipeline, stageName, startDate)
    print(stageTask, pipeline, stageName, startDate)
    if len(stageTask) > 0 and int(stageTask[0]['status']) > 0:
        ## Should not rerun
        return False, "Already run"
    elif len(stageTask) > 0 and stageTask[0]['status'] == 0:
        return False, "It is running"

    if dependJob is None or len(dependJob) == 0:
        return True, None

    dependOnTask = get_pipeline_task(dependJob['pipeline'], dependJob['stage'], startDate)
    if len(dependOnTask) and dependOnTask[0]['status'] > 0:
        return True, None
    else:
        return False, f"{dependJob['pipeline']}.{dependJob['stage']} is not completed"

def can_run2(pipeline, stageName, startDate): 
    dependJobs = DEPENDENCY_GRAPH[pipeline][stageName]['dependJobs']
    
    for j in dependJobs:
      checkInfo = can_run1(pipeline, stageName, j, startDate)
      if checkInfo[0] == False:
        return False, checkInfo[1]
    if len(dependJobs) == 0:
      return can_run1(pipeline, stageName, None, startDate)
    return True, None


