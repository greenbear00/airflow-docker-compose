from datetime import datetime
import rootpath
import socket
from dateutil.relativedelta import relativedelta

def runner(target_date:datetime):
    print(f"hello!, {target_date}")


def rootpath_detect(target_date:datetime):
    print(f"hostname = {socket.gethostname()}")
    print(f"rootpath = {rootpath.detect()}")
    
    print(target_date-relativedelta(months = 1))

