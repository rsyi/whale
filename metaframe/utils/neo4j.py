def combine_where_clauses(clauses):
    if clauses:
        return 'WHERE ' + ' AND '.join(clauses)
    else:
        return ''

