"""
API module to interact with Google spreadsheets
In order to create a new spreadsheet, share the spreadsheet with the
'client_email' in your credentials json file with write permissions.


"""
import os
from ocs_ci.framework import config
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSpreadSheetAPI(object):
    """
    A class to interact with Google Spreadsheet
    """
    def __init__(self, sheet_name, sheet_index):
        # use creds to create a client to interact with the Google Drive API
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        google_api = os.path.expanduser(config.RUN['google_api_secret'])
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            google_api, scope
        )
        client = gspread.authorize(creds)
        self.sheet = client.open(sheet_name).get_worksheet(sheet_index)

    def update_cell(self, row, col, value):
        """
        Updates a row:col in a given spreadsheet
        """
        self.sheet.update_cell(row, col, value)

    def print_sheet(self):
        """
        prints all the contents of a given spreadsheet
        """
        list_of_hashes = self.sheet.get_all_records()
        print(list_of_hashes)

    def get_cell_value(self, row, col):
        """
        Retrieves value from a spreadsheet
        """
        return self.sheet.cell(row, col).value

    def insert_row(self, value, row_index=2):
        """
        Inserts a row with content to be updated
        """
        return self.sheet.insert_row(value, row_index)

    def update_title(self, title):
        """
        Updates the sheet name
        """
        return self.sheet.update_title(title)

    def append_row(self, value):
        """
        Appends a row with content
        """
        return self.sheet.append_row(value)

    def clear_sheet(self):
        """
        Deletes all contents of the sheet
        """
        return self.sheet.clear()
