import pandas as pd
import requests
import os


class DatasetCreator:
    def __init__(self, input_file_path):
        self.input_file_path = input_file_path
        self.df_input = None
        self.df_output = None
        self.load_input_dataset()

    def load_input_dataset(self,  filePath=None):
        """Loads the input dataset."""
        file_to_load = filePath if filePath is not None else self.input_file_path
        if file_to_load.endswith('.sav'):
            df_input = pd.read_spss(file_to_load)
        elif file_to_load.endswith('.csv'):
            df_input = pd.read_csv(file_to_load)
        elif file_to_load.endswith('.xlsx'):
            df_input = pd.read_excel(file_to_load)
        if filePath is None:
            self.df_input = df_input
        else:
            return df_input

    def add_description_column(self, api_url=None, description_file_path=None):
        """Adds a description column using an external API or a description file."""
        descriptions = []
        for column in self.df_output['Column_Name']:
            if api_url:
                description = self.get_description_from_api(column, api_url=api_url)
            else:
                description = 'Description not available'

            # If description is not available from the API, fetch from the file
            if description == 'Description not available' and description_file_path:
                description = self.get_description_from_file(column, description_file_path)

            descriptions.append(description)
        self.df_output['Description'] = descriptions

    @staticmethod
    def get_description_from_api(self, column_name, api_url):
        """Fetches description of a column name from an external API."""
        try:
            response = requests.get(f"{api_url}/get_description?column={column_name}")
            if response.status_code == 200:
                return response.json().get('description', 'No description available')
        except requests.RequestException:
            pass
        return 'Description not available'

    def get_description_from_file(self, column_name, description_file_path):
        """Fetches description of a column name from a description file."""
        df_descriptions = self.load_input_dataset(description_file_path)
        description_dict = df_descriptions.set_index('Column_Name')['Description'].to_dict()
        return description_dict.get(column_name, 'No description available')
    def create_output_dataset(self):
        """Creates the output dataset from the input dataset."""
        # Assuming 'Column_Name' is the list of column names from the input dataset
        column_names = self.df_input.columns.tolist()
        print(f"column_names={column_names}")
        # Initialize the output DataFrame
        self.df_output = pd.DataFrame(column_names, columns=['Column_Name'])
        self.add_description_column()
        summary_df = []
        for name in column_names:
            column_summary = self.create_statistical_summary(column_name=name)
            column_summary_df = pd.DataFrame(column_summary, index=[name])
            summary_df.append(column_summary_df)
        all_summaries = pd.concat(summary_df)
        self.add_description_column(description_file_path='/Users/komalgilani/Desktop/PhD_Maastricht/Datasets/Patient_RegistryUM/description_registry.xlsx')
        self.df_output = pd.concat([self.df_output, all_summaries.reset_index(drop=True)], axis=1)
        base_name = os.path.basename(self.input_file_path)
        output_file_path = base_name + '_summary.csv'
        self.save_output_dataset(output_file_path)

    def create_statistical_summary(self, column_name):
        """Creates a statistical summary for a specified column in the first dataset."""
        if column_name not in self.df_input.columns:
            return f"Column '{column_name}' not found in DataFrame."

        summary = self.df_input[column_name].describe(include='all').to_dict()
        return summary

    def save_output_dataset(self, output_file_path):
        """Saves the output dataset to a file."""
        self.df_output.to_csv(output_file_path)
