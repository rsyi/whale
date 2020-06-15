def combine_where_clauses(and_clauses=[], or_clauses=[]):
    clauses = []
    if and_clauses:
        clauses.append('(' + ' AND '.join(and_clauses) + ')')
    if or_clauses:
        clauses.append('(' + ' OR '.join(or_clauses) + ')')

    if clauses:
        return 'WHERE ' + ' AND '.join(clauses)
    else:
        return ''
