import requests
import os
from bs4 import BeautifulSoup



class checker:

    links_with_test_name_found = None
    links_with_id_found = None


    def search_and_download(self, folder_path,
                            file_name=None,
                            # run_id=None,
                            # test_name=None,
                            output_folder='.',
                            base_url='http://magna002.ceph.redhat.com/'):
        """
        Searches for a file by name in an Apache repository folder and all its descendant folders, and downloads the file
        to the specified output folder if found.
        """

        # if run_id and test_name:
        #     file_name = None

        # Build the URL for the folder
        if folder_path[0] == '/':
            folder_path = folder_path[1:]
        url = os.path.join(base_url, folder_path)
        print(f"requesting url {url}")

        # Make a request to the URL to get the folder contents
        response = requests.get(url)
        print(f'resp = {response.status_code}')
        if response.status_code != 200:
            raise ValueError(f"Failed to get contents of {url}")

        # Parse the folder contents as HTML to extract links

        soup = BeautifulSoup(response.content, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        links.reverse()
        print(f"got links = {links}")

        # Look for the file by name in the folder contents
        if file_name in links:
            # Build the URL for the file
            file_url = os.path.join(url, file_name)
            print(f"requesting file by its url '{file_url}'")

            # Download the file to the specified output folder
            response = requests.get(file_url)
            if response.status_code == 200:
                output_file_path = os.path.join(output_folder, file_name)
                print(f"put file to path '{output_file_path}'")
                with open(output_file_path, 'wb') as f:
                    f.write(response.content)
                    return output_file_path

        # links_with_id = [link for link in links if link.count(str(run_id)) and link != self.links_with_id_found]
        # links_with_id.reverse()
        # if len(links_with_id):
        #     self.links_with_id_found = links_with_id[0]
        #     print(f'run_id found {links_with_id}')
        #     links = links_with_id
        #
        # links_with_test_names = [link for link in links if link.count(str(test_name)) and link != self.links_with_test_name_found]
        # links_with_test_names.reverse()
        # if len(links_with_test_names):
        #     self.links_with_test_name_found = links_with_test_names[0]
        #     print(f'test_name found {links_with_test_names}')
        #     links = links_with_test_names


        # Recursively search descendant folders for the file
        for link in links:
            if link.endswith('/'):
                descendant_folder_path = os.path.join(folder_path, link)
                found_path = self.search_and_download(base_url=base_url, folder_path=descendant_folder_path, file_name=file_name,
                                                 output_folder=output_folder)
                if found_path:
                    return found_path

        # File not found in this folder or any descendant folders
        return None

    def search_apache_repo(self, folder_path, folder_name, base_url='http://magna002.ceph.redhat.com/'):
        """
        Searches for a folder by name in an Apache repository folder and all its descendant folders
        """
        if folder_path[0] == '/':
            folder_path = folder_path[1:]

        # Build the URL for the folder
        url = os.path.join(base_url, folder_path)

        # Make a request to the URL to get the folder contents
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Failed to get contents of {url}")

        # Parse the folder contents as HTML to extract links
        soup = BeautifulSoup(response.content, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        links.reverse()

        # Look for the folder by name in the folder contents
        if folder_name in links:
            return os.path.join(folder_path, folder_name)

        # Recursively search descendant folders for the folder
        for link in links:
            if link.endswith('/'):
                descendant_folder_path = os.path.join(folder_path, link)
                found_path = self.search_apache_repo(base_url=base_url, folder_path=descendant_folder_path, folder_name=folder_name)
                if found_path:
                    return found_path

        # Folder not found in this folder or any descendant folders
        return None


if __name__ == '__main__':
    # search_and_download('http://magna002.ceph.redhat.com/',
    #                     'ocsci-jenkins/openshift-clusters/rperiyas-2504/rperiyas-2504_20230425T054833/logs/ui_logs_dir_1682581380/screenshots_ui/',
    #                     '2023-04-28T08-34-14.986118.png',
    #                     '.')

    # checker = checker()
    # checker.search_and_download(
    #     run_id='1682581380',
    #     folder_path='ocsci-jenkins/openshift-clusters/rperiyas-2504/rperiyas-2504_20230425T054833/logs/ui_logs_dir_1682581380/screenshots_ui/',
    #     file_name='2023-04-28T08-34-14.986118.png',
    # )

    checker = checker()
    # checker.search_apache_repo(folder_path='ocsci-jenkins/openshift-clusters/rperiyas-2504/',
    #                            folder_name='ui_logs_dir_1682581380/screenshots_ui/')

    checker.search_and_download(
        # run_id='1682581380',
        # test_name = 'test_odf_storagesystems_ui',
        folder_path='ocsci-jenkins/openshift-clusters/rperiyas-2504/rperiyas-2504_20230425T054833/logs/ui_logs_dir_1682581380/screenshots_ui/',
        file_name='2023-04-28T08-34-14.986118.png',
    )
