from generate_summary import *
# Press the green button in the gutter to run the script.
import generate_summary
if __name__ == '__main__':
    filepath = '/Users/user_name/Downloads/patient_register_UMV1.sav'
    dc = DatasetCreator()
    dc.create_output_dataset(filepath)


