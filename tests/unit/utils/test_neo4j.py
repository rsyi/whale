from metaframe.utils.neo4j import combine_where_clauses


AND_CLAUSES = ['and_clause_1', 'and_clause_2']
OR_CLAUSES = ['or_clause_1', 'or_clause_2']

def test_combine_where_clauses_with_result():
    statement =  combine_where_clauses(
        and_clauses=AND_CLAUSES,
        or_clauses=OR_CLAUSES)
    assert 'WHERE' in statement


def test_combine_where_clauses_with_no_result():
    statement = combine_where_clauses()
    assert statement == ''
