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
    :return: tuple of two elements
    """
    fields = line.split(delimiter)
    age = int(fields[column_a])
    num_friends = int(fields[column_b])
    return age, num_friends
