from datetime import datetime

def runner(target_date:datetime):
    print(f"hello!, {target_date}")


def rootpath_detect(target_date:datetime):
    import rootpath
    print(rootpath.detect())

