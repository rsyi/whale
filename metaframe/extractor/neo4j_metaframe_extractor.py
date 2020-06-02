from itertools import zip_longest
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from metaframe.models.table_metadata import TableMetadata, ColumnMetadata


class Neo4jMetaframeExtractor(Neo4jExtractor):

    CYPHER_QUERY = """
        MATCH (db:Database)<-[:CLUSTER_OF]-(cluster:Cluster)
        <-[:SCHEMA_OF]-(schema:Schema)<-[:TABLE_OF]-(table:Table)
        OPTIONAL MATCH (table)-[:DESCRIPTION]->(table_description:Description)
        OPTIONAL MATCH (schema)-[:DESCRIPTION]->(schema_description:Description)
        OPTIONAL MATCH (table)-[:TAGGED_BY]->(tags:Tag) WHERE tags.tag_type='default'
        WITH db, cluster, schema, schema_description, table, table_description, COLLECT(DISTINCT tags.key) as tags
        OPTIONAL MATCH (table)-[:TAGGED_BY]->(badges:Tag) WHERE badges.tag_type='badge'
        WITH db, cluster, schema, schema_description, table, table_description, tags, COLLECT(DISTINCT badges.key) AS
        badges
        OPTIONAL MATCH (table)-[read:READ_BY]->(user:User)
        WITH db, cluster, schema, schema_description, table, table_description, tags, badges, SUM(read.read_count) AS
        total_usage,
        COUNT(DISTINCT user.email) as unique_usage
        OPTIONAL MATCH (table)-[:COLUMN]->(col:Column)
        OPTIONAL MATCH (col)-[:DESCRIPTION]->(col_description:Description)
        WITH db, cluster, schema, schema_description, table, table_description, tags, badges, total_usage, unique_usage,
        COLLECT(col.name) AS column_names,
        COLLECT(col_description.description) AS column_descriptions,
        COLLECT(col.type) AS column_types,
        COLLECT(col.sort_order) AS column_sort_orders
        OPTIONAL MATCH (table)-[:LAST_UPDATED_AT]->(time_stamp:Timestamp)
        OPTIONAL MATCH (table)<-[:BELONG_TO_TABLE]-(watermark:Watermark)
        WITH db, cluster, schema, schema_description, table, table_description, tags, badges, total_usage, unique_usage, column_names, column_descriptions, column_types, column_sort_orders, time_stamp, COLLECT(watermark) AS watermarks

        RETURN
        db.name as database,
        cluster.name AS cluster,
        schema.name AS schema,
        schema_description.description AS schema_description,
        table.name AS name,
        table.key AS key,
        table.is_view AS is_view,
        table_description.description AS description,
        time_stamp.last_updated_timestamp AS last_updated_timestamp,
        column_names,
        column_descriptions,
        column_types,
        column_sort_orders,
        total_usage,
        unique_usage,
        tags,
        badges,
        watermarks
        ORDER BY table.name;
        """

    def init(self, conf):
        # type: (ConfigTree) -> None
        """
        Establish connections and import data model class if provided
        :param conf:
        """
        self.conf = conf.with_fallback(Neo4jExtractor.DEFAULT_CONFIG)
        self.graph_url = conf.get_string('graph_url')
        self.cypher_query = Neo4jMetaframeExtractor.CYPHER_QUERY
        self.driver = self._get_driver()

        self._extract_iter = None

    def _get_extract_iter(self):
        with self.driver.session() as session:
            if not hasattr(self, 'results'):
                self.results = session.read_transaction(self._execute_query)

            for result in self.results:
                # Parse watermark information.
                partition_columns = []
                for watermark in result['watermarks']:
                    partition_columns.append(watermark['partition_key'])

                # Parse column information.
                column_names = result['column_names']
                column_descriptions = result['column_descriptions']
                column_types = result['column_types']
                column_sort_orders = result['column_sort_orders']
                zipped_columns = zip_longest(
                    column_names,
                    column_descriptions,
                    column_types,
                    column_sort_orders)

                column_metadatas = []
                for column_name, column_description, column_type, column_sort_order \
                        in zipped_columns:
                    if column_name in partition_columns:
                        is_partition_column = True
                    else:
                        is_partition_column = False
                    column_metadatas.append(ColumnMetadata(
                        name=column_name,
                        description=column_description,
                        col_type=column_type,
                        sort_order=column_sort_order,
                        is_partition_column=is_partition_column,
                    ))

                yield TableMetadata(
                    database=result['database'],
                    cluster=result['cluster'],
                    schema=result['schema'],
                    name=result['name'],
                    description=result['description'],
                    columns=column_metadatas,
                    is_view=result['is_view'],
                    tags=result['tags'],
                )
                yield result

    def get_scope(self):
        return 'extractor.neo4j_metaframe'
