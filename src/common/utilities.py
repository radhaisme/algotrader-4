import os
import pathlib


def rename_files(dir_path):
    """
    Rename all files for a given extension within a folder
    Args:
        dir_path:

    Returns:

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
    iter_islast(iterable) -> generates (item, islast) pairs
    https://code.activestate.com/recipes/392015-finding-the-last-item-in-a-loop/
    Generates pairs where the first element is an item from the iterable
    source and the second element is a boolean flag indicating if it is the
    last item in the sequence.
    """
    it = iter(iterable)
    prev = it.__next__()
    for item in it:
        yield prev, False
        prev = item
    yield prev, True


if __name__ == '__main__':
    my_path = "/media/sf_D_DRIVE/Trading/data/clean_fxcm"
    # rename_files(my_path)
    zero_bytes_files(my_path, action='print')
