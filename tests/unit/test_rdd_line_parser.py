from taming_pyspark.utils import rdd_line_parser as parsers


def test_file_to_csv_tuple_with_exclusions():
    # Arrange
    test_case = 'TX, Austin, USA, 78709, GOOGL, Income, 235000, L4, HIRE_YEAR, 2019, BACKEND ENGINEERING, 6'
    indices = {
        'start': 0,
        'end': 10,
        'excludes': [2, 8, 9]
    }
    # Act
    actual = parsers.file_to_csv_tuple(line=test_case, delimiter=', ', **indices)
    # Assert
    assert len(actual) == 7
    assert 'GOOGL' in actual
    assert 'USA' not in actual


def test_file_to_csv_tuple_without_exclusions():
    # Arrange
    test_case = 'TX, Austin, USA, 78709, GOOGL, Income, 235000, L4, HIRE_YEAR, 2019, BACKEND ENGINEERING, 6'
    indices = {
        'start': 0,
        'end': 10
    }
    # Act
    actual = parsers.file_to_csv_tuple(line=test_case, delimiter=', ', **indices)
    # Assert
    assert len(actual) == 10
