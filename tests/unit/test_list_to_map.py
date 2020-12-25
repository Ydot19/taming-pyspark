from taming_pyspark.utils import list_to_map as util


def test_creat_map_from_str_list_no_special_character():
    # Arrange
    sample = ['676\t538\t4\t892685437', '721\t262\t3\t877137285', '913\t209\t2\t881367150']
    # Act
    actual = util.creat_map_from_str_list(sample, '\t', 0, 2)
    # Assert
    expect = {'676': '4', '721': '3', '913': '2'}
    assert isinstance(actual, dict)
    assert expect == actual
