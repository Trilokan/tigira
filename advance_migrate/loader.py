import os
import time

start = 0
end = 100

for i in range(1, 1000):
    cmd = "nohup python /home/ubuntu/sel_1/tigira/advance_migrate/main.py {0} {1} &".format(start, end)
    os.system(cmd)
    start = start + 100
    end = end + 100
    time.sleep(300)
