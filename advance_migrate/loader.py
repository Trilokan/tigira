import os
import time

start = 50000
end = 51000

for i in range(1, 200):
    cmd = "nohup python /home/ubuntu/sel_1/tigira/advance_migrate/main.py {0} {1} &".format(start, end)
    os.system(cmd)
    start = start + 1000
    end = end + 1000
    time.sleep(200)
