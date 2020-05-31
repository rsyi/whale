import logging
from metaframe import main
logging.basicConfig(format='%(message)s', level=logging.INFO)
logging.getLogger("pyhive").setLevel(logging.WARNING)
logging.getLogger("databuilder.task.task").setLevel(logging.WARNING)
main()
