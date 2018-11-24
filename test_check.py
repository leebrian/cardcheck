#!/usr/local/bin/python

"""
A couple of tests for card check.
Used for debugging.
Dumps out a full inventory to data dir.
"""

import check
from pathlib import Path
import json
import numpy
import platform


def main():
    print('starting')

    test_suite()

    print('hello')


def test_platform_node():
    host_name = platform.node()
    print(host_name)
    assert (host_name is not None), "Host name from platform node should not be null."


def test_card_lib():

    cardLibraryFile = Path(check.DATA_DIR_NAME + "AllCards.json")

    with cardLibraryFile.open() as file:
        cardLibraryDict = json.load(file)

    return cardLibraryDict


def test_inventory():
    strTodayFileName = check.today_csv_file_name()
    print(strTodayFileName)
    df_inventory, dfOldCards, strOldFileName = check.buildCompareDFs(
        strTodayFileName)
    return df_inventory


def test_sort_category(card_lib, inventory):
    """deckbox doesn't export color, so I add a column used for sorting"""
    inventory["SortCategory"] = inventory["Name"].apply(
        check.lookupSortCategory, args=(card_lib,))

    return inventory


def test_write_full_inventory(inventory):
    inventory.to_csv(check.DATA_DIR_NAME + 'full_inventory.csv')


def test_default_numpy():
    test_val = check.default_numpy(numpy.int64(7))
    assert (test_val == 7), "Default to 7 meand default numpy is working"
    try:
        test_val = check.default_numpy(numpy.float64(7.0))
    except TypeError:
        pass


def test_suite():
    card_lib = test_card_lib()
    df_inventory = test_inventory()
    df_full_inventory = test_sort_category(card_lib, df_inventory)
    test_write_full_inventory(df_full_inventory)
    test_default_numpy()
    test_platform_node()


if __name__ == "__main__":
    main()
