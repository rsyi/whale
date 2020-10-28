import csv
import logging
import os
from datetime import datetime
from databuilder.task.task import DefaultTask
from whale.utils import paths

LOGGER = logging.getLogger(__name__)


class WhaleTask(DefaultTask):
    def init(self, conf):
        super(WhaleTask, self).init(conf)
        self.database_name = conf.get("loader.whale.database_name")

    def run(self):
        """
        Runs a task but appends the count as an attribute
        :return:
        """
        try:
            self.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record = self.extractor.extract()
            self.count = 1
            while record:
                record = self.transformer.transform(record)
                if not record:
                    record = self.extractor.extract()
                    continue
                self.loader.load(record)
                record = self.extractor.extract()
                self.count += 1

        finally:
            self._closer.close()

    def save_stats(self):
        LOGGER.info("Saving task-level statistics.")
        has_headers = os.path.isfile(paths.TABLE_COUNT_PATH)

        with open(paths.TABLE_COUNT_PATH, "a") as csvfile:
            headers = ["start_time", "database_name", "number_tables"]
            writer = csv.DictWriter(
                csvfile, delimiter=",", lineterminator="\n", fieldnames=headers
            )

            if not has_headers:
                writer.writeheader()

            writer.writerow(
                {
                    "start_time": self.start_time,
                    "database_name": self.database_name,
                    "number_tables": self.count,
                }
            )
