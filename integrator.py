# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from generate_summary import *
# Press the green button in the gutter to run the script.
import generate_summary
if __name__ == '__main__':
    filepath = '/Users/komalgilani/Downloads/patient_register_UMV1.sav'
    dc = DatasetCreator()
    dc.create_output_dataset(filepath)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
