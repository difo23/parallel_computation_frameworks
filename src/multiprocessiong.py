from multiprocessing import Pool
import pandas as pd
from pathlib import Path
import time
from functools import wraps


def print_timing(func):
    '''
    create a timing decorator function
    use
    @print_timing
    just above the function you want to time
    '''
    @wraps(func)  # improves debugging
    def wrapper(*arg):
        start = time.perf_counter()  # needs python3.3 or higher
        result = func(*arg)
        end = time.perf_counter()
        fs = '{} nodes {} took {:.3f} microseconds'
        print(fs.format(func.__name__, arg[2] , (end - start)*1000000))
        return result
    return wrapper


def read_dataset(filename=Path("./data/athlete_events.csv").resolve()):
    return pd.read_csv(filename)


def take_mean_age(year_and_group):
    year, group = year_and_group
    return pd.DataFrame({"Age": group["Age"].mean()}, index=[year])


with Pool(4) as p:
    athlete_events = read_dataset()
    results = p.map(take_mean_age, athlete_events.groupby("Year"))


result_df = pd.concat(results)

# Function to apply a function over multiple cores
athlete_events = read_dataset()


@print_timing
def parallel_apply(apply_func, groups, nb_cores):
    with Pool(nb_cores) as p:
        results = p.map(apply_func, groups)
    return pd.concat(results)


# Parallel apply using 1 core
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 1)

# Parallel apply using 2 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 2)

# Parallel apply using 4 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 4)
