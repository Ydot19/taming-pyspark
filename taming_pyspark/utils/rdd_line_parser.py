def csv_line_to_len_2_tuple(
        line: str,
        delimiter: str = ',',
        column_a: int = 2,
        column_b: int = 3) \
        -> (int, int):
    """
    Returns a tuple of length two for a csv with minimum 2 columns
    :param line: file line to read
    :param delimiter: delimiter pattern
    :param column_a: csv column index starting from zero
    :param column_b: csv column index starting from zero
    :return: tuple of two integers
    """
    fields = line.split(delimiter)
    int_a = int(fields[column_a])
    int_b = int(fields[column_b])
    return int_a, int_b


def file_to_csv_tuple(line: str, delimiter: str = ',', **column_indexes) -> tuple:
    """
    Converts string to tuple of length n
    :param delimiter: Characters in string to separate on
    :param line: String from a file
    :param column_indexes: Keyword arguments
        start - integer identifying the starting index
        end - integer identifying the ending index
        excludes - list of indexes to exclude
    :return:
    """
    before_exclusions: list[str] = line.split(delimiter)[column_indexes['start']: column_indexes['end']]
    remove_indices: list[int] = column_indexes['excludes'] if 'excludes' in column_indexes else []
    after_exclusions = [i for j, i in enumerate(before_exclusions) if j not in remove_indices]
    return tuple(after_exclusions)
