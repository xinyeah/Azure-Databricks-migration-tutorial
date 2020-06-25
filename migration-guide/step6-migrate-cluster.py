import sys
import os
import subprocess
import json
from subprocess import call, check_output

EXPORT_PROFILE = "primary"
IMPORT_PROFILE = "secondary"

# Get all clusters info from old workspace
clusters_out = check_output(["databricks", "clusters", "list", "--profile", EXPORT_PROFILE])
clusters_info_list = str(clusters_out.decode(encoding="utf-8")).splitlines()
print("Printting Cluster info List")
print(clusters_info_list)

# Create a list of all cluster ids
clusters_list = []

for cluster_info in clusters_info_list:
   if cluster_info != '':
      clusters_list.append(cluster_info.split(None, 1)[0])

# Create a list of mandatory / optional create request elements
cluster_req_elems = ["num_workers","autoscale","cluster_name","spark_version","spark_conf","node_type_id","driver_node_type_id","custom_tags","cluster_log_conf","spark_env_vars","autotermination_minutes","enable_elastic_disk"]
print("Printing Cluster element List")
print (cluster_req_elems)
print(str(len(clusters_list)) + " clusters found in the primary site" )

print ("---------------------------------------------------------")
# Try creating all / selected clusters in new workspace with same config as in old one.
cluster_old_new_mappings = {}
i = 0
for cluster in clusters_list:
   i += 1
   print("Checking cluster " + str(i) + "/" + str(len(clusters_list)) + " : " +str(cluster))
   cluster_get_out_f = check_output(["databricks", "clusters", "get", "--cluster-id", str(cluster), "--profile", EXPORT_PROFILE])
   cluster_get_out=str(cluster_get_out_f.decode(encoding="utf-8"))
   print ("Got cluster config from old workspace")
   print (cluster_get_out)
    # Remove extra content from the config, as we need to build create request with allowed elements only
   cluster_req_json = json.loads(cluster_get_out)
   cluster_json_keys = cluster_req_json.keys()

   #Don't migrate Job clusters
   if cluster_req_json['cluster_source'] == u'JOB' :
      print ("Skipping this cluster as it is a Job cluster : " + cluster_req_json['cluster_id'] )
      print ("---------------------------------------------------------")
      continue

      #cluster_req_json.pop(key, None)
      for key in cluster_json_keys:
        if key not in cluster_req_elems:
         print (cluster_req_json)
         #cluster_del_item=cluster_json_keys .keys()
         cluster_req_json.popitem(key, None)

   # Create the cluster, and store the mapping from old to new cluster ids

   #Create a temp file to store the current cluster info as JSON
   strCurrentClusterFile = "tmp_cluster_info.json"

   #delete the temp file if exists
   if os.path.exists(strCurrentClusterFile) :
      os.remove(strCurrentClusterFile)

   fClusterJSONtmp = open(strCurrentClusterFile,"w+")
   fClusterJSONtmp.write(json.dumps(cluster_req_json))
   fClusterJSONtmp.close()

   cluster_create_out = check_output(["databricks", "clusters", "create", "--json-file", strCurrentClusterFile , "--profile", IMPORT_PROFILE])
   cluster_create_out_json = json.loads(cluster_create_out)
   cluster_old_new_mappings[cluster] = cluster_create_out_json['cluster_id']

   print ("Cluster create request sent to secondary site workspace successfully")
   print ("---------------------------------------------------------")

   #delete the temp file if exists
   if os.path.exists(strCurrentClusterFile) :
      os.remove(strCurrentClusterFile)

print ("Cluster mappings: " + json.dumps(cluster_old_new_mappings))
print ("All done")
print ("P.S. : Please note that all the new clusters in your secondary site are being started now!")
print ("       If you won't use those new clusters at the moment, please don't forget terminating your new clusters to avoid charges")