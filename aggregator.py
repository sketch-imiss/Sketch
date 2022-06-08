def aggregator(result, partition, sample, sprobability):
    if partition == 0:
        dict_result = result
    elif partition == 1:
        dict_result = dict()
        for dic in result:
            for key in dic:
                if key not in dict_result:
                    dict_result[key] = 0
                dict_result[key] += dic[key]

    if sample == 0:
        return dict_result
    elif sample == 1:
        for key in dict_result:
            dict_result[key] /= sprobability

    return dict_result