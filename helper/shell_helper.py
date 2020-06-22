import datetime
import subprocess
import platform
import time


def execute_command(cmd, timeout=0, err_log_file=""):
    if timeout:
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    sub = subprocess.Popen(cmd, stdin=subprocess.PIPE, bufsize=4096, shell=True)
    while sub.poll() is None:
        if timeout:
            if end_time <= datetime.datetime.now():
                if platform.system() == 'Windows':
                    kill_cmd = "taskkill /PID {} /T /F".format(sub.pid)
                else:
                    kill_cmd = "kill -9 {}".format(sub.pid)
                subprocess.Popen(kill_cmd, shell=True)
                # sub.kill()
                return -1
        time.sleep(10)
    if sub.returncode != 0 and err_log_file:
        with open(err_log_file, "w") as writer:
            writer.write(str(sub.stdout.read()))

    return sub.returncode
