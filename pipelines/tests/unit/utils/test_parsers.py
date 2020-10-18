from whale.utils.parsers import parse_ugc

METRIC_STATEMENT = "METRIC_PAYLOAD"
TEST_BLOB = f"""
```
filler
```

```metrics
{METRIC_STATEMENT}
```
"""

def test_parse_ugc():
    sections = parse_ugc(TEST_BLOB)
    assert sections['defined_metrics'][0] == "\n" + METRIC_STATEMENT + "\n"
