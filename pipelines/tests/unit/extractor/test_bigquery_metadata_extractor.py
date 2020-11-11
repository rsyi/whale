import logging
import unittest

from mock import patch, Mock
from pyhocon import ConfigFactory

from databuilder import Scoped
from whale.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from whale.models.table_metadata import TableMetadata

logging.basicConfig(level=logging.INFO)


NO_DATASETS = {"kind": "bigquery#datasetList", "etag": "1B2M2Y8AsgTpgAmY7PhCfg=="}
ONE_DATASET = {
    "kind": "bigquery#datasetList",
    "etag": "yScH5WIHeNUBF9b/VKybXA==",
    "datasets": [
        {
            "kind": "bigquery#dataset",
            "id": "your-project-here:empty",
            "datasetReference": {
                "datasetId": "empty",
                "projectId": "your-project-here",
            },
            "location": "US",
        }
    ],
}  # noqa
NO_TABLES = {
    "kind": "bigquery#tableList",
    "etag": "1B2M2Y8AsgTpgAmY7PhCfg==",
    "totalItems": 0,
}
ONE_TABLE = {
    "kind": "bigquery#tableList",
    "etag": "Iaqrz2TCDIANAOD/Xerkjw==",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "your-project-here:fdgdfgh.nested_recs",
            "tableReference": {
                "projectId": "your-project-here",
                "datasetId": "fdgdfgh",
                "tableId": "nested_recs",
            },
            "type": "TABLE",
            "labels": {"label1": "test1", "label2": "label2"},
            "creationTime": "1557578974009",
        }
    ],
    "totalItems": 1,
}  # noqa
ONE_VIEW = {
    "kind": "bigquery#tableList",
    "etag": "Iaqrz2TCDIANAOD/Xerkjw==",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "your-project-here:fdgdfgh.abab",
            "tableReference": {
                "projectId": "your-project-here",
                "datasetId": "fdgdfgh",
                "tableId": "abab",
            },
            "type": "VIEW",
            "view": {"useLegacySql": False},
            "creationTime": "1557577874991",
        }
    ],
    "totalItems": 1,
}  # noqa
TIME_PARTITIONED = {
    "kind": "bigquery#tableList",
    "etag": "Iaqrz2TCDIANAOD/Xerkjw==",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "your-project-here:fdgdfgh.other",
            "tableReference": {
                "projectId": "your-project-here",
                "datasetId": "fdgdfgh",
                "tableId": "other",
            },
            "type": "TABLE",
            "timePartitioning": {"type": "DAY", "requirePartitionFilter": False},
            "creationTime": "1557577779306",
        }
    ],
    "totalItems": 1,
}  # noqa
TABLE_DATE_RANGE = {
    "kind": "bigquery#tableList",
    "etag": "Iaqrz2TCDIANAOD/Xerkjw==",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "your-project-here:fdgdfgh.other_20190101",
            "tableReference": {
                "projectId": "your-project-here",
                "datasetId": "fdgdfgh",
                "tableId": "date_range_20190101",
            },
            "type": "TABLE",
            "creationTime": "1557577779306",
        },
        {
            "kind": "bigquery#table",
            "id": "your-project-here:fdgdfgh.other_20190102",
            "tableReference": {
                "projectId": "your-project-here",
                "datasetId": "fdgdfgh",
                "tableId": "date_range_20190102",
            },
            "type": "TABLE",
            "creationTime": "1557577779306",
        },
    ],
    "totalItems": 2,
}  # noqa
TABLE_DATA = {
    "kind": "bigquery#table",
    "etag": "Hzc/56Rp9VR4Y6jhZApD/g==",
    "id": "your-project-here:fdgdfgh.test",
    "selfLink": "https://www.googleapis.com/bigquery/v2/projects/your-project-here/datasets/fdgdfgh/tables/test",
    "tableReference": {
        "projectId": "your-project-here",
        "datasetId": "fdgdfgh",
        "tableId": "test",
    },
    "schema": {
        "fields": [
            {"name": "test", "type": "STRING", "description": "some_description"},
            {"name": "test2", "type": "INTEGER"},
            {"name": "test3", "type": "FLOAT", "description": "another description"},
            {"name": "test4", "type": "BOOLEAN"},
            {"name": "test5", "type": "DATETIME"},
        ]
    },
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numRows": "0",
    "creationTime": "1557577756303",
    "lastModifiedTime": "1557577756370",
    "type": "TABLE",
    "location": "EU",
    "labels": {"label_1": "test_label_1", "label_2": "test_label_2"},
}  # noqa
NO_SCHEMA = {
    "kind": "bigquery#table",
    "etag": "Hzc/56Rp9VR4Y6jhZApD/g==",
    "id": "your-project-here:fdgdfgh.no_schema",
    "selfLink": "https://www.googleapis.com/bigquery/v2/projects/your-project-here/datasets/fdgdfgh/tables/no_schema",
    "tableReference": {
        "projectId": "your-project-here",
        "datasetId": "fdgdfgh",
        "tableId": "no_schema",
    },
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numRows": "0",
    "creationTime": "1557577756303",
    "lastModifiedTime": "1557577756370",
    "type": "TABLE",
    "location": "EU",
}  # noqa
NO_COLS = {
    "kind": "bigquery#table",
    "etag": "Hzc/56Rp9VR4Y6jhZApD/g==",
    "id": "your-project-here:fdgdfgh.no_columns",
    "selfLink": "https://www.googleapis.com/bigquery/v2/projects/your-project-here/datasets/fdgdfgh/tables/no_columns",
    "tableReference": {
        "projectId": "your-project-here",
        "datasetId": "fdgdfgh",
        "tableId": "no_columns",
    },
    "schema": {},
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numRows": "0",
    "creationTime": "1557577756303",
    "lastModifiedTime": "1557577756370",
    "type": "TABLE",
    "location": "EU",
}  # noqa
VIEW_DATA = {
    "kind": "bigquery#table",
    "etag": "E6+jjbQ/HsegSNpTEgELUA==",
    "id": "gerard-cloud-2:fdgdfgh.abab",
    "selfLink": "https://www.googleapis.com/bigquery/v2/projects/gerard-cloud-2/datasets/fdgdfgh/tables/abab",
    "tableReference": {
        "projectId": "gerard-cloud-2",
        "datasetId": "fdgdfgh",
        "tableId": "abab",
    },
    "schema": {
        "fields": [
            {"name": "test", "type": "STRING"},
            {"name": "test2", "type": "INTEGER"},
            {"name": "test3", "type": "FLOAT"},
            {"name": "test4", "type": "BOOLEAN"},
            {"name": "test5", "type": "DATETIME"},
        ]
    },
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numRows": "0",
    "creationTime": "1557577874991",
    "lastModifiedTime": "1557577874991",
    "type": "VIEW",
    "view": {
        "query": "SELECT * from `gerard-cloud-2.fdgdfgh.test`",
        "useLegacySql": False,
    },
    "location": "EU",
}  # noqa
NESTED_DATA = {
    "kind": "bigquery#table",
    "etag": "Hzc/56Rp9VR4Y6jhZApD/g==",
    "id": "your-project-here:fdgdfgh.test",
    "selfLink": "https://www.googleapis.com/bigquery/v2/projects/your-project-here/datasets/fdgdfgh/tables/test",
    "tableReference": {
        "projectId": "your-project-here",
        "datasetId": "fdgdfgh",
        "tableId": "test",
    },
    "schema": {
        "fields": [
            {
                "name": "nested",
                "type": "RECORD",
                "fields": [
                    {
                        "name": "nested2",
                        "type": "RECORD",
                        "fields": [{"name": "ahah", "type": "STRING"}],
                    }
                ],
            }
        ]
    },
    "type": "TABLE",
    "location": "EU",
}  # noqa
ENTRY = {
    "name": "projects/your-project-here/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvd2hhbGUtZGV2LTI5NDgxMi9kYXRhc2V0cy90ZXN0aW5nL3RhYmxlcy90YWJsZTE",
    "type": "TABLE",
    "description": "this is a table",
    "schema": {
        "columns": [
            {
                "type": "STRING",
                "description": "testcol1",
                "mode": "NULLABLE",
                "column": "col1",
            },
            {
                "type": "DOUBLE",
                "description": "testcol2",
                "mode": "NULLABLE",
                "column": "col2",
            },
            {
                "type": "INT64",
                "description": "testcol3",
                "mode": "NULLABLE",
                "column": "col3",
            },
            {"type": "STRING", "mode": "NULLABLE", "column": "col4"},
        ]
    },
    "sourceSystemTimestamps": {
        "createTime": "2020-11-06T13:27:22.537Z",
        "updateTime": "2020-11-06T15:34:57.325Z",
    },
    "linkedResource": "//bigquery.googleapis.com/projects/your-project-here/datasets/testing/tables/table1",
    "bigqueryTableSpec": {"tableSourceType": "BIGQUERY_TABLE"},
    "integratedSystem": "BIGQUERY",
}
TAGS = {
    "tags": [
        {
            "name": "projects/your-project-here/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvd2hhbGUtZGV2LTI5NDgxMi9kYXRhc2V0cy90ZXN0aW5nL3RhYmxlcy90YWJsZTE/tags/CXy_PbcgFLIaW",
            "template": "projects/your-project-here/locations/europe-west2/tagTemplates/demo_tag",
            "fields": {
                "demo2": {"displayName": "demo2", "boolValue": "true"},
                "demo1": {"displayName": "demo1", "stringValue": "test1", "order": 1},
            },
            "templateDisplayName": "demo-tag",
        },
        {
            "name": "projects/your-project-here/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvd2hhbGUtZGV2LTI5NDgxMi9kYXRhc2V0cy90ZXN0aW5nL3RhYmxlcy90YWJsZTE/tags/CYPqPyHt4oku",
            "template": "projects/your-project-here/locations/europe-west2/tagTemplates/demo_tag",
            "fields": {
                "demo1": {"displayName": "demo1", "stringValue": "test1", "order": 1},
                "demo2": {"displayName": "demo2", "boolValue": "true"},
            },
            "column": "test4",
            "templateDisplayName": "demo-tag",
        },
    ]
}


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class MockBigQueryClient:
    def __init__(self, dataset_list_data, table_list_data, table_data):
        self.ds_execute = Mock()
        self.ds_execute.execute.return_value = dataset_list_data
        self.ds_list = Mock()
        self.ds_list.list.return_value = self.ds_execute
        self.list_execute = Mock()
        self.list_execute.execute.return_value = table_list_data
        self.get_execute = Mock()
        self.get_execute.execute.return_value = table_data
        self.tables_method = Mock()
        self.tables_method.list.return_value = self.list_execute
        self.tables_method.get.return_value = self.get_execute

    def datasets(self):
        return self.ds_list

    def tables(self):
        return self.tables_method


class MockDataCatalogClient:
    def __init__(self, entry_data, tags_data):
        self.entry = entry_data
        self.tags = tags_data

    def lookup_entry(self, request):
        return self.entry

    def list_tags(self, request):
        return self.tags


@patch("google.auth.default", lambda scopes: ["dummy", "dummy"])
class TestBigQueryMetadataExtractor(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        config_dict = {
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.PROJECT_ID_KEY
            ): "your-project-here",
        }

        self.conf = ConfigFactory.from_dict(config_dict)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_can_handle_datasets(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(NO_DATASETS, None, None)
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_empty_dataset(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(ONE_DATASET, NO_TABLES, None)
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsNone(result)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_accepts_dataset_filter_by_label(self, mock_datacatalogue, mock_bigquery):
        config_dict = {
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.PROJECT_ID_KEY
            ): "your-project-here",
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.FILTER_KEY
            ): "label.key:value",
        }
        conf = ConfigFactory.from_dict(config_dict)

        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_TABLE, TABLE_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(Scoped.get_scoped_conf(conf=conf, scope=extractor.get_scope()))
        result = extractor.extract()
        self.assertIsInstance(result, TableMetadata)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_table_without_schema(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_TABLE, NO_SCHEMA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()

        self.assertEqual(result.database, "bigquery")
        self.assertEqual(result.cluster, "your-project-here")
        self.assertEqual(result.schema, "fdgdfgh")
        self.assertEqual(result.name, "nested_recs")
        self.assertEqual(result.description, "")
        self.assertEqual(result.columns, [])
        self.assertEqual(result.is_view, False)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_table_without_columns(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(ONE_DATASET, ONE_TABLE, NO_COLS)
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()

        self.assertEqual(result.database, "bigquery")
        self.assertEqual(result.cluster, "your-project-here")
        self.assertEqual(result.schema, "fdgdfgh")
        self.assertEqual(result.name, "nested_recs")
        self.assertEqual(result.description, "")
        self.assertEqual(result.columns, [])
        self.assertEqual(result.is_view, False)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_view(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_VIEW, VIEW_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()
        self.assertIsInstance(result, TableMetadata)
        self.assertEqual(result.is_view, True)

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_normal_table(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_TABLE, TABLE_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()

        self.assertEqual(result.database, "bigquery")
        self.assertEqual(result.cluster, "your-project-here")
        self.assertEqual(result.schema, "fdgdfgh")
        self.assertEqual(result.name, "nested_recs")
        self.assertEqual(result.description, "")
        self.assertEqual(result.is_view, False)
        self.assertEqual(
            result.tags,
            {
                "name": "projects/your-project-here/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvd2hhbGUtZGV2LTI5NDgxMi9kYXRhc2V0cy90ZXN0aW5nL3RhYmxlcy90YWJsZTE/tags/CXy_PbcgFLIaW",
                "template": "projects/your-project-here/locations/europe-west2/tagTemplates/demo_tag",
                "fields": {
                    "demo2": {"displayName": "demo2", "boolValue": "true"},
                    "demo1": {
                        "displayName": "demo1",
                        "stringValue": "test1",
                        "order": 1,
                    },
                },
                "templateDisplayName": "demo-tag",
            },
        )
        self.assertEqual(
            result.labels, {"label_1": "test_label_1", "label_2": "test_label_2"}
        )

        first_col = result.columns[0]
        self.assertEqual(first_col.name, "test")
        self.assertEqual(first_col.type, "STRING")
        self.assertEqual(first_col.description, "some_description")
        self.assertEqual(first_col.tags, None)

        fourth_col = result.columns[3]
        self.assertEqual(
            fourth_col.tags,
            {
                "name": "projects/your-project-here/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvd2hhbGUtZGV2LTI5NDgxMi9kYXRhc2V0cy90ZXN0aW5nL3RhYmxlcy90YWJsZTE/tags/CYPqPyHt4oku",
                "template": "projects/your-project-here/locations/europe-west2/tagTemplates/demo_tag",
                "fields": {
                    "demo1": {
                        "displayName": "demo1",
                        "stringValue": "test1",
                        "order": 1,
                    },
                    "demo2": {"displayName": "demo2", "boolValue": "true"},
                },
                "column": "test4",
                "templateDisplayName": "demo-tag",
            },
        )

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_table_with_nested_records(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_TABLE, NESTED_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )
        result = extractor.extract()

        first_col = result.columns[0]
        self.assertEqual(first_col.name, "nested")
        self.assertEqual(first_col.type, "RECORD")
        second_col = result.columns[1]
        self.assertEqual(second_col.name, "nested.nested2")
        self.assertEqual(second_col.type, "RECORD")
        third_col = result.columns[2]
        self.assertEqual(third_col.name, "nested.nested2.ahah")
        self.assertEqual(third_col.type, "STRING")

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_keypath_and_pagesize_can_be_set(self, mock_datacatalogue, mock_bigquery):
        config_dict = {
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.PROJECT_ID_KEY
            ): "your-project-here",
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.PAGE_SIZE_KEY
            ): 200,
            "extractor.bigquery_table_metadata.{}".format(
                BigQueryMetadataExtractor.KEY_PATH_KEY
            ): "/tmp/doesnotexist",
        }
        conf = ConfigFactory.from_dict(config_dict)

        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, ONE_TABLE, TABLE_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()

        with self.assertRaises(FileNotFoundError):
            extractor.init(
                Scoped.get_scoped_conf(conf=conf, scope=extractor.get_scope())
            )

    @patch("whale.extractor.base_bigquery_extractor.build")
    @patch("whale.extractor.base_bigquery_extractor.datacatalog_v1")
    def test_table_part_of_table_date_range(self, mock_datacatalogue, mock_bigquery):
        mock_bigquery.return_value = MockBigQueryClient(
            ONE_DATASET, TABLE_DATE_RANGE, TABLE_DATA
        )
        mock_datacatalogue.DataCatalogClient.return_value = MockDataCatalogClient(
            ENTRY, TAGS
        )
        extractor = BigQueryMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )

        count = 0
        result = extractor.extract()
        table_name = result.name
        while result:
            count += 1
            result = extractor.extract()

        self.assertEqual(count, 1)
        self.assertEqual(table_name, "date_range_")
