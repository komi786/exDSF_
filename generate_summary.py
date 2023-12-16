import pandas as pd
import requests
import os
import pyreadstat
from ydata_profiling import ProfileReport
from tqdm import tqdm


def get_description_from_api(column_name, api_url):
    """Fetches description of a column name from an external API."""
    try:
        response = requests.get(f"{api_url}/get_description?column={column_name}")
        if response.status_code == 200:
            return response.json().get('description', 'No description available')
    except requests.RequestException:
        return 'Description not available'


def get_description_from_file(column_name, description_file_path):
    """Fetches description of a column name from a description file."""
    df_descriptions = pd.read_csv(description_file_path)
    description_dict = df_descriptions.set_index('Column_Name')['Description'].to_dict()
    return description_dict.get(column_name, 'No description available')


class DatasetCreator:
    def __init__(self):
        self.input_file_path = None
        self.df_input = None
        self.df_output = None

    def load_input_dataset(self, filepath=None):
        """Loads the input dataset."""
        file_to_load = self.input_file_path if filepath is None else filepath
        if file_to_load.endswith('.sav'):
            self.df_input = pd.read_spss(file_to_load)
            # Optionally save to Excel if needed
            # self.df_input.to_excel("patient_registry.xlsx")
        elif file_to_load.endswith('.csv'):
            self.df_input = pd.read_csv(file_to_load)
        elif file_to_load.endswith('.xlsx'):
            self.df_input = pd.read_excel(file_to_load)
        else:
            raise ValueError("Unsupported file format.")

    def add_description_column(self, api_url=None, description_file_path=None):
        """Adds a description column using an external API or a description file."""
        descriptions = []
        for column in self.df_output['Column_Name']:
            if api_url:
                description = self.get_description_from_api(column, api_url)
            elif description_file_path:
                description = self.get_description_from_file(column, description_file_path)
            else:
                description = 'Description not available'
            descriptions.append(description)
        self.df_output['Description'] = descriptions

    def create_output_dataset(self,file_path):
        """Creates the output dataset from the input dataset."""
        self.load_input_dataset(file_path)
        column_names = self.df_input.columns.tolist()
        self.df_output = pd.DataFrame(column_names, columns=['Column_Name'])

        # Initialize progress bar
        pbar = tqdm(total=len(column_names), desc="Generating Description")

        # Iterate through columns with progress update
        summary_df = []
        for col in column_names:
            column_summary = self.df_input[col].describe().to_frame(col).T
            summary_df.append(column_summary)
            pbar.update(1)  # Update progress

        pbar.close()  # Close the progress bar

        summary_df = pd.concat(summary_df).reset_index(drop=True)
        self.df_output = pd.concat([self.df_output, summary_df], axis=1)
        filename = os.path.basename(file_path)
        output_file_path = f"{filename}_summary.csv"
        self.save_output_dataset(output_file_path)

    def save_output_dataset(self, output_file_path):
        """Saves the output dataset to a file."""
        self.df_output.to_csv(output_file_path)

    def create_profile(self, file_path):
        """Generates a profile report."""
        self.load_input_dataset(file_path)
        profile = ProfileReport(self.df_input, explorative=True)
        profile.to_file("profile_report.html")
