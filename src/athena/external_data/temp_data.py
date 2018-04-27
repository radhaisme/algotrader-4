
import pandas as pd


def format_to_sql_database(file_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master databases
    Returns:

    """
    try:
        chunksize = 100000
        for df in pd.read_csv(file_path, chunksize=chunksize, iterator=True, compression='gzip'):
            return df
    except pd.errors.EmptyDataError as e:
        return e


if __name__ == '__main__':

    # 3. Check files integrity after the clean up.
    good_file = "/media/sf_D_DRIVE/Trading/data/clean_fxcm/8.csv.gz"
    bad_file = "/media/sf_D_DRIVE/Trading/data/clean_fxcm/21.csv.gz"
    x = format_to_sql_database(good_file)
    print(x)


