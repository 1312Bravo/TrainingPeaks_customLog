import numpy as np

# Weighted quantile
def weighted_quantile(values, weights, quantile):

    sorter = np.argsort(values)
    values_sorted = values[sorter]
    weights_sorted = weights[sorter]

    cumulative_normalized_weights  = np.cumsum(weights_sorted) / np.sum(weights_sorted)
    quantile_value = np.interp(quantile, cumulative_normalized_weights, values_sorted)

    return quantile_value[0]

# Weighted mean
def weighted_mean(values, weights):

    if len(values) == 0:
        return 0.0

    sum_weighted_values = np.sum(values * weights)
    sum_weights = np.sum(weights)

    weighted_mean = 1/sum_weights * sum_weighted_values
    return weighted_mean