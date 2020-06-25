import sys
import os
import subprocess
from subprocess import call, check_output

EXPORT_PROFILE = "primary"
IMPORT_PROFILE = "secondary"

# Get a list of all folders
folder_list_out = check_output(["databricks", "workspace", "ls", "--profile", EXPORT_PROFILE])
folder_list = (folder_list_out.decode(encoding="utf-8")).splitlines()

print (folder_list)

#Libraries are not included with these APIs / commands.

for folder in folder_list:
  print (("Trying to migrate workspace for folder ") + folder)

  #For local env: Linux
  # subprocess.call(str("mkdir -p ") + str(folder), shell = True)
  #For local env: Windows
  subprocess.call(str("mkdir ") + str(folder), shell = True)
  export_exit_status = call("databricks workspace export_dir /" + str(folder) + " ./" + str(folder) + " --profile " + EXPORT_PROFILE, shell = True)

  if export_exit_status==0:
    print ("Export Success")
    import_exit_status = call("databricks workspace import_dir ./" + str(folder) + " /" + str(folder) + " --profile " + IMPORT_PROFILE, shell=True)
    if import_exit_status==0:
      print ("Import Success")
    else:
      print ("Import Failure")
  else:
    print ("Export Failure")
print ("All done")