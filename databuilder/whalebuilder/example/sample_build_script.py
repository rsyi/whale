from pyhocon import ConfigFactory

from databuilder.task.task import DefaultTask
from whalebuilder.extractor.presto_loop_extractor import PrestoLoopExtractor
from whalebuilder.loader.metaframe_loader import MetaframeLoader
from whalebuilder.transformer.markdown_transformer import MarkdownTransformer


conf = ConfigFactory.from_dict({
    'extractor.presto_loop.conn_string': 'PUT CONN STRING HERE',
    'extractor.presto_loop.is_table_metadata_enabled': True,
    'extractor.presto_loop.is_full_extraction_enabled': True,
    'extractor.presto_loop.is_watermark_enabled': False,
    'extractor.presto_loop.is_stats_enabled': False,
    'extractor.presto_loop.is_analyze_enabled': False,
    'extractor.presto_loop.database': None,
    'extractor.presto_loop.catalog': None,
    'extractor.presto_loop.included_schemas': None,
    'extractor.presto_loop.excluded_schemas': None,
    'loader.metaframe.database_name': 'presto-test',
})

task = DefaultTask(
    extractor=PrestoLoopExtractor(),
    transformer=MarkdownTransformer(),
    loader=MetaframeLoader(),
)
task.init(conf)
task.run()
