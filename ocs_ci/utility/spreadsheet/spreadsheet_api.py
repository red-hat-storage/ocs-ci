"""
API module to interact with Google spreadsheets
In order to create a new spreadsheet, share the spreadsheet with:
ocs-qe@ocs-qe.iam.gserviceaccount.com with write permissions.


"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSpreadSheetAPI(object):
    """
    A class to interact with Google Spreadsheet
    """
    def __init__(self, sheet_name):
        # use creds to create a client to interact with the Google Drive API
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'client_secret.json', scope
        )
        client = gspread.authorize(creds)

        self.sheet = client.open(sheet_name).sheet1

    def update_sheet(self, row, col, value):
        """
        Updates a row:col in a given spreadsheet
        """
        self.sheet.update_cell(row, col, value)

    def print_sheet(self):
        list_of_hashes = self.sheet.get_all_records()
        print(list_of_hashes)

    def get_cell_value(self, row, col):
        return self.sheet.cell(row, col).value
