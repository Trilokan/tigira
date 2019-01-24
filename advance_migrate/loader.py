import os
import time
import psutil

def findProcessIdByName(processName):
    '''
    Get a list of all the PIDs of a all the running process whose name contains
    the given string processName
    '''

    listOfProcessObjects = []

    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['pid', 'name', 'create_time'])
            # Check if process name contains the given name string.
            if processName.lower() in pinfo['name'].lower():
                listOfProcessObjects.append(pinfo)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    return listOfProcessObjects;


start = 0
end = 100

for i in range(1, 1000):
    cmd = "nohup python /home/ubuntu/sel_1/tigira/advance_migrate/main.py {0} {1} &".format(start, end)
    os.system(cmd)
    start = start + 100
    end = end + 100
    listOfProcessIds = findProcessIdByName('main')
    if listOfProcessIds > 2:
        time.sleep(500)
