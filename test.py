reports = [
        {'ga:campaign','ga:source','ga:medium',},
        {'ga:country','ga:city',},
        {'ga:language',},]

dimensions = [{'name': 'ga:date'}]
print (dimensions)
print (type(dimensions))
dimensions.extend([{'name': dim} for dim in reports])
print (dimensions)
print (type(dimensions))

