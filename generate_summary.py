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
            self.df_input.to_csv("data_analysis.csv",sep=',',index=False)
        elif file_to_load.endswith('.csv'):
            self.df_input = pd.read_csv(file_to_load)
        elif file_to_load.endswith('.xlsx'):
            self.df_input = pd.read_excel(file_to_load)
        else:
            raise ValueError("Unsupported file format.")

    def add_description_column(self, api_url=None, description_file_path=None):
        """Adds a description column using an external API or a description file."""
        descriptions = []
        for column in self.df_output['Name']:
            if api_url:
                description = self.get_description_from_api(column, api_url)
            elif description_file_path:
                description = self.get_description_from_file(column, description_file_path)
            else:
                description = 'Description not available'
            descriptions.append(description)
        self.df_output['Description'] = descriptions

    def get_description_from_file(self,column_name, description_file_path):
        """Fetches description of a column name from a description file."""
        df_descriptions = pd.read_excel(description_file_path)
        description_dict = df_descriptions.set_index('Name')['Description'].to_dict()
        return description_dict.get(column_name, 'No description available')
    def create_output_dataset(self,file_path):
        """Creates the output dataset from the input dataset."""

        # Initialize progress bar


        self.load_input_dataset(file_path)
        column_names = self.df_input.columns.tolist()
        self.df_output = pd.DataFrame({'Name': column_names})
        pbar = tqdm(total=len(column_names), desc="Generating Description")
        # Iterate through columns with progress update
        summary_df = []
        attributes = self.generate_data_analysis("data_analysis.csv")
        for col_name,attribute in attributes.items():
            new_row = {
                "Name" : col_name,
                "Type": attribute["type"],
                "Count/Length":attribute["n"],
                "Distinct_Value": attribute["n_distinct"],
                "Percent_Distinct": attribute["p_distinct"],
                "Missing_Value": attribute["n_missing"],
                "Percent_Missing": attribute["p_missing"],
                "Is_Unique": attribute["is_unique"],
                "Memory_Usage": attribute["memory_size"]
            }
            summary_df.append(new_row)
            pbar.update(1) # Update progress


        self.df_output = pd.DataFrame(summary_df)
        #summary_df = pd.concat(summary_df).reset_index(drop=True)
        self.add_description_column(description_file_path="/Users/komalgilani/Desktop/PhD_Maastricht/Datasets/Patient_RegistryUM/description_registry.xlsx")
        filename = os.path.basename(file_path)
        filename = os.path.splitext(filename)[0]
        output_file_path = f"{filename}_summary.csv"
        self.save_output_dataset(output_file_path)
        pbar.close()  # Close the progress bar
        print(f"Summary written to {output_file_path}")

    def save_output_dataset(self, output_file_path):
        """Saves the output dataset to a file."""
        self.df_output.to_csv(output_file_path)

    def generate_data_analysis(self, file_path):
        """Generates a profile report and returns column attributes."""
        self.load_input_dataset(file_path)
        profile = ProfileReport(self.df_input, minimal=True, explorative=True)
        attributes = profile.get_description().variables
        return attributes

