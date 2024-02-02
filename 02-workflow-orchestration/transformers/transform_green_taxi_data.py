if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import inflection

@transformer
def transform(data, *args, **kwargs):

    old_cols = data.columns
    data.columns = [inflection.underscore(col) for col in old_cols]
    columns = data.columns

    print(data.columns.tolist(), '\n')

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    print('unique "vendor_id" values = ', data['vendor_id'].unique().tolist(), '\n')

    print('renamed columns = ', len(list(set(old_cols) - set(columns))))

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, '"vendor_id" column not found!'
    assert output['passenger_count'].isin([0]).sum() == 0, '"passenger_count" values equal or less than 0 found!'
    assert output['trip_distance'].isin([0]).sum() == 0, '"trip_distance" values equal or less than 0 found!'