import numpy as np

# Weighted quantile value for specified quantile
def get_weighted_quantile_value(quantile, values, weights):

    sorter = np.argsort(values)
    values_sorted = values[sorter]
    weights_sorted = weights[sorter]

    cumulative_weights  = np.cumsum(weights_sorted) / np.sum(weights_sorted)
    quantile_value = np.interp(quantile, cumulative_weights, values_sorted)

    return quantile_value

# Weighted percentile rank for specifid value
def get_weighted_percentile_rank(value, values, weights):

    sorter = np.argsort(values)
    values_sorted = values[sorter]
    weights_sorted = weights[sorter]

    cumulative_weights = np.cumsum(weights_sorted) / np.sum(weights_sorted)
    percentile_rank = np.interp(value, values_sorted, cumulative_weights)
    percentile_rank = np.clip(percentile_rank, 0, 1) 

    return percentile_rank

# Weighted mean
def get_weighted_mean(values, weights):

    if len(values) == 0:
        return 0.0

    sum_weighted_values = np.sum(values * weights)
    sum_weights = np.sum(weights)

    weighted_mean = 1/sum_weights * sum_weighted_values
    return weighted_mean