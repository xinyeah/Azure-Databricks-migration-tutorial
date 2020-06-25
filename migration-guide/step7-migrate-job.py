import sys
import os
import subprocess
import json
from subprocess import call, check_output


EXPORT_PROFILE = "primary"
IMPORT_PROFILE = "secondary"

# Please replace the old to new cluster id mappings from cluster migration output
cluster_old_new_mappings = {"0227-120427-tryst214": "0229-032632-paper88"}

# Get all jobs info from old workspace
try:
  jobs_out = check_output(["databricks", "jobs", "list", "--profile", EXPORT_PROFILE])
  jobs_info_list = jobs_out.splitlines()
except:
  print("No jobs to migrate")
  sys.exit(0)

# Create a list of all job ids
jobs_list = []
for jobs_info in jobs_info_list:
  jobs_list.append(jobs_info.split(None, 1)[0])

# Optionally filter job ids out manually, so as to create only required ones in new workspace

# Create each job in the new workspace based on corresponding settings in the old workspace

for job in jobs_list:
  print("Trying to migrate " + str(job.decode("utf-8")))

  job_get_out = check_output(["databricks", "jobs", "get", "--job-id", str(job.decode("utf-8")), "--profile", EXPORT_PROFILE])
  print("Got job config from old workspace")

  job_req_json = json.loads(job_get_out)  
  job_req_settings_json = job_req_json['settings']

  # Remove schedule information so job doesn't start before proper cutover
  job_req_settings_json.pop('schedule', None)

  # Replace old cluster id with new cluster id, if job configured to run against an existing cluster
  if 'existing_cluster_id' in job_req_settings_json:
    if job_req_settings_json['existing_cluster_id'] in cluster_old_new_mappings:
      job_req_settings_json['existing_cluster_id'] = cluster_old_new_mappings[job_req_settings_json['existing_cluster_id']]
    else:
      print("Mapping not available for old cluster id ") + job_req_settings_json['existing_cluster_id']
      continue

  call(["databricks", "jobs", "create", "--json", json.dumps(job_req_settings_json), "--profile", IMPORT_PROFILE])
  print("Sent job create request to new workspace successfully")

print("All done")