import json

# %run yelp_challenge/util_functions_spark.py
# n_dict = sc.parallelize(n_dict, 16).map(flatten_attributes_field).collect()
# dict_business = sc.parallelize(df_b_collected, 4).map(f).collect()

# def pop_attributes(x):                                                                   
#     x = x.asDict(True)
#     a, b = x.pop('attributes'), x.pop('business_id')
#     b = {'business_id': b}                                                                                                           
#     return {**a, **b}

# for column in df_a.columns:
#     df_a = df_a.withColumn(column, F.when(df_a[column] == 'True', F.lit(1)).when(df_a[column] == 'False', 0).otherwise(df_a[column]))

# checking null percentage
# a = dict()
# for column in df.columns:
#     a[column] = df.filter(df[column].isNull()).count()/59774 * 100

def drop_not_restaurants(e_list):
    def check_keys(e_dict):
        keys = ('RestaurantsGoodForGroups', 'RestaurantsDelivery', 'RestaurantsReservations', 'RestaurantsTableService', 'RestaurantsTableService')

        for key in keys:
            e = e_dict.get(key, None)
            if e is not None:
                return True
        return False

    return [e_dict for e_dict in e_list if check_keys(e_dict)]

def flatten_field_internal(e_dict, field, internal_fields):
    internal_dict = e_dict.pop('attributes__' + field)
    if internal_dict:
       internal_dict = internal_dict.replace("'", '"').replace('False', '"False"').replace('True', '"True"')
       internal_dict = json.loads(internal_dict)
       internal_dict = {'attributes__{}__{}'.format(field, key): value for key,value in internal_dict.items()}
    else:
       internal_dict = {'attributes__{}__{}'.format(field, internal_field): None for internal_field in internal_fields}

    return {**e_dict, **internal_dict}


def flatten_attributes_field(e_dict): 
    try:
        e_dict = {'attributes__' + key: value for key,value in e_dict.items()}

        e_dict = flatten_field_internal(e_dict, 'BusinessParking', ('garage', 'lot', 'street', 'valet', 'validated'))
        e_dict = flatten_field_internal(e_dict, 'Ambience', ('romantic', 'intimate', 'classy', 'hipster', 'divey', 'touristy', 'trendy', 'upscale', 'casual'))
        e_dict = flatten_field_internal(e_dict, 'GoodForMeal', ('dessert', 'latenight', 'lunch', 'dinner', 'breakfast', 'brunch'))
        e_dict = flatten_field_internal(e_dict, 'Music', ('dj', 'background_music', 'no_music', 'karaoke', 'live', 'video', 'jukebox'))
    except AttributeError:
        print(e_dict)

    return e_dict
