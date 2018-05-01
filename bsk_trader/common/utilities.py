import os
import pathlib
from itertools import tee, islice, chain
import datetime
from functools import wraps


def rename_files(dir_path):
    """
    Rename all files for a given extension within a folder

    """

    dir_path = pathlib.Path(dir_path)
    counter = 0
    all_files = dir_path.glob('**/*.gz')

    for old_path in dir_path.glob('**/*.gz'):
        # get the components of the path
        parts = pathlib.Path(old_path).parts
        parent = pathlib.Path(old_path).parent

        # Construct new file path
        wk = parts[-1]
        yr = parts[-2]
        sym = parts[-3]
        new_name = sym + '_' + yr + '_' + wk
        new_path = parent / new_name

        # Rename
        os.rename(old_path, new_path)
        counter += 1
        print('Doing {} out of {}'.format(counter, len(list(all_files))))


def zero_bytes_files(dir_path, action=None):
    """

    Args:
        dir_path:
        action:
    Returns:

    """
    zeros = []
    dir_path = pathlib.Path(dir_path)

    for each_file in dir_path.glob('**/*.gz'):
        print('Checking file: {}'.format(each_file))

        if os.stat(each_file).st_size == 100000:   #size in bytes
            zeros.append(each_file)

    if action is None:
        print('Done !!!')
    elif action == 'print':
        print(zeros)
    elif action == 'delete':
        for to_delete in zeros:
            os.remove(to_delete)
            print('File deleted: {}'.format(to_delete))

    return zeros


def iter_islast(iterable):
    """
    Generates pairs where the first element is an item from the iterable
    source and the second element is a boolean flag indicating if it is the
    last item in the sequence.

    https://code.activestate.com/recipes/392015-finding-the-last-item-in-a-loop/

    Returns: (item, islast)
    """
    it = iter(iterable)
    prev = it.__next__()
    for item in it:
        yield prev, False
        prev = item
    yield prev, True


def previous_and_next(some_iterable):
    """
    Generates tupple  with three consecutive elements of an iterable
    source where the first element is the previous element of the iteration,
    the second element is the current element and the last is the next.

    https://stackoverflow.com/a/1012089/3512107

    Returns: (previous, item, next)
    """
    prevs, items, nexts = tee(some_iterable, 3)
    prevs = chain([None], prevs)
    nexts = chain(islice(nexts, 1, None), [None])
    return zip(prevs, items, nexts)


def fn_timer(function):
    """
    Define a decorator that measures the elapsed time in running the function.

    http://www.marinamele.com/7-tips-to-time-python-scripts-and-control-memory-and-cpu-usage

    Returns: print the elapsed time

    """
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = datetime.datetime.now()
        result = function(*args, **kwargs)
        t1 = datetime.datetime.now()
        print("Total time running {}: {}".format(function.__name__, t1 - t0))
        return result
    return function_timer

if __name__ == '__main__':
    my_path = "/media/sf_D_DRIVE/Trading/data/clean_fxcm"
    # rename_files(my_path)
    zero_bytes_files(my_path, action='print')
