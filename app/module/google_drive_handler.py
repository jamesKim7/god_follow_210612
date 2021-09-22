from __future__ import print_function
import pickle
import os
import shutil
import json
import requests
import time
from datetime import datetime as dt

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']


class GoogleDriveHandler:

    def __init__(self):
        self.creds = None
        self.access_token = None
        self.service = None
        self.drive_id = None
        self.root_id = None
        self.dir_root = ''
        self.dir_upload = ''
        self.dir_finished = ''
        self.dir_download = ''

    @staticmethod
    def get_root_dir():
        # root_dir 은 'god_follow_' 가 들어가야 한다.
        path_elements = os.getcwd().split('\\')
        index_root_dir = 0
        for idx, element in enumerate(path_elements):
            if element.find('god_follow_') > -1:
                index_root_dir = idx
                break
        root_dir = ('\\'.join(path_elements[:index_root_dir + 1])).replace('\\', '/')

        return root_dir

    def get_config(self):
        self.dir_root = self.get_root_dir()
        print(os.getcwd())

        with open(f'{self.dir_root}/app/files/config.json', 'r') as f:
            json_data = json.load(f)
            self.dir_upload = json_data['google_drive_handler']['dir_upload']
            self.dir_finished = json_data['google_drive_handler']['dir_finished']
            self.dir_download = json_data['google_drive_handler']['dir_download']

    def set_token(self):
        path_root = self.dir_root
        creds = None
        access_token = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(f'{path_root}/app/files/token.pickle'):
            with open(f'{path_root}/app/files/token.pickle', 'rb') as token:
                creds = pickle.load(token)
                access_token = creds.token
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    f'{path_root}/app/files/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                access_token = creds.token
            # Save the credentials for the next run
            with open(f'{path_root}/app/files/token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.creds = creds
        self.access_token = access_token

    def build_service(self):
        creds = self.creds

        self.service = build('drive', 'v3', credentials=creds)

    @staticmethod
    def get_local_file_list(dir_path):
        file_list = os.listdir(dir_path)

        result_list = []

        for file_name in file_list:
            temp_list = [dir_path, file_name]
            result_list.append(temp_list)

        return result_list

    def get_drive_file_list_all(self):
        service = self.service

        page_token = None

        file_list = []

        while True:
            response = service.files().list(pageSize=100,
                                            fields="nextPageToken, files(id, name, mimeType, parents)",
                                            pageToken=page_token
                                            ).execute()
            items = response.get('files', [])

            if not items:
                print('No files found.')
            else:
                print('Files:')
                for item in items:
                    print(f'{item["name"]} ({item["id"]})')
                    file_list.append([item['id'], item['name'], item['mimeType'], item['parents']])
            page_token = response.get('nextPageToken', None)
            print(page_token)
            if page_token is None:
                break

        for file_e in file_list:
            file_id = file_e[0]
            file_name = file_e[1]
            file_mime = file_e[2]
            file_parent = file_e[3]
            if file_name == 'root' and file_mime == 'application/vnd.google-apps.folder':
                self.root_id = file_id
                self.drive_id = file_parent[0]

        return file_list

    def get_drive_folder_e(self, folder_name):
        service = self.service

        folder_list = []
        page_token = None

        while True:
            response = service.files().list(q='mimeType = "application/vnd.google-apps.folder"',
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageSize=100,
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                print('Found folder: %s (%s)' % (file.get('id'), file.get('name')))
                if file['name'] == folder_name:
                    folder_list.append([file['id'], file.get('name')])

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        if len(folder_list) == 1:

            return folder_list[0]

        else:
            print('폴더가 개수가 1개가 아닙니다.| {}'.format(folder_list))

            return 0

    def get_drive_files_list_in_folder(self, folder_e):
        service = self.service

        id_folder = folder_e[0]
        name_folder = folder_e[1]

        file_list = []
        page_token = None
        while True:
            response = service.files().list(q='"{}" in parents'.format(id_folder),
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name, mimeType, parents)',
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                print('Found file: %s (%s) %s %s' % (file.get('id'), file.get('name'), file['mimeType'], file['parents']))
                file_list.append([file['id'], file['name'], file['mimeType'], file['parents']])
                # print(file)
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        return file_list

    def create_folder(self, options):
        service = self.service

        parent_id = options['parent_id']
        file_name = options['file_name']

        file_metadata = {
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        file = service.files().create(body=file_metadata,
                                      fields='id').execute()
        print('File Created: %s' % file)

        return file

    def move_file(self, options):
        service = self.service

        file_id = options['file_id']
        folder_id = options['folder_id']
        parent_id = options['parent_id']

        # Move the file to the new folder
        file_e = service.files().update(fileId=file_id,
                                      addParents=folder_id,
                                      removeParents=parent_id,
                                      fields='id, name, mimeType, parents').execute()

        return file_e

    def upload(self, options):

        access_token = self.access_token

        # 폴더 및 파일 지정
        dir_path = self.dir_upload
        dir_finished = self.dir_finished
        parent_id = options['parent_id']

        file_list = self.get_local_file_list(dir_path)

        for idx, row in enumerate(file_list):

            dir_path = row[0]
            filename = row[1]

            time_s = time.time()

            print('{} is uploading...'.format(filename))
            filesize = os.path.getsize('{}\\{}'.format(dir_path, filename))

            # 1. Retrieve session for resumable upload.

            headers = {
                "Authorization": "Bearer " + access_token,
                "Content-Type": "application/json; charset=UTF-8"
            }
            params = {
                "name": filename,
                "mimeType": "text/sql",
                "parents": [parent_id]

            }
            r = requests.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
                headers=headers,
                data=json.dumps(params)
            )
            print(r.text)
            location = r.headers['Location']

            # 2. Upload the file.

            with open('{}\\{}'.format(dir_path, filename), 'rb') as filedata:
                headers = {"Content-Range": "bytes 0-" + str(filesize - 1) + "/" + str(filesize)}
                r = requests.put(
                    location,
                    headers=headers,
                    data=filedata
                )

            time_p = time.time() - time_s
            print('{} is uploaded!'.format(filename), int(time_p))

            # upload 한 파일 옮기기, upload_neede > finished
            shutil.move('{}\\{}'.format(dir_path, filename), '{}\\{}'.format(dir_finished, filename))

    def download(self, options):
        service = self.service
        dir_download = self.dir_download
        download_needed_dir_id = options['download_needed_dir_id']
        finished_dir_id = options['finished_dir_id']

        folder_e = [download_needed_dir_id, 0]

        file_list = self.get_drive_files_list_in_folder(folder_e)

        if len(file_list) >= 1:

            # 리스트 순환하며 파일 다운로드
            for file_e in file_list:
                file_id = file_e[0]
                file_name = file_e[1]
                file_mime = file_e[2]
                file_parent = file_e[3][0]
                request = service.files().get_media(fileId=file_id)
                fh = io.FileIO('{}\\{}'.format(dir_download, file_name), mode='wb')
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print("Download %d%%." % int(status.progress() * 100))

            for file_e in file_list:
                file_id = file_e[0]
                file_name = file_e[1]
                file_mime = file_e[2]
                file_parent = file_e[3][0]

                options = {
                    'file_id': file_id,
                    'folder_id': finished_dir_id,
                    'parent_id': file_parent
                }
                file_e = self.move_file(options)
                print('File Moved: {}'.format(file_e))

            return 1

        else:
            print('파일 리스트 ERROR| {}'.format(file_list))

            return 0


if __name__ == '__main__':

    GDH = GoogleDriveHandler()
    GDH.get_config()
    GDH.set_token()
    GDH.build_service()

    # setting
    date = dt.today().strftime('%Y%m%d')

    mode = 'upload'

    if mode == 'upload':

        # check today dir exist, if not create it
        file_list = GDH.get_drive_file_list_all()

        flag_date_dir_exist = 0
        download_needed_dir_id = ''

        for file_e in file_list:
            file_id = file_e[0]
            file_name = file_e[1]
            file_mime = file_e[2]
            file_parent = file_e[3]

            if file_name == date and file_mime == 'application/vnd.google-apps.folder':
                flag_date_dir_exist = 1

                # Get download_needed folder id
                for file_e2 in file_list:
                    if file_e2[1] == 'download_needed' and file_e2[2] == 'application/vnd.google-apps.folder' and\
                            file_e2[3][0] == file_id:
                        download_needed_dir_id = file_e2[0]

                break

        if not flag_date_dir_exist:
            options = {
                'parent_id': GDH.root_id,
                'file_name': date
            }
            new_folder_e = GDH.create_folder(options)
            new_folder_id = new_folder_e['id']

            options = {
                'parent_id': new_folder_id,
                'file_name': 'download_needed'
            }
            download_needed_dir_id = GDH.create_folder(options)

            options = {
                'parent_id': new_folder_id,
                'file_name': 'finished'
            }
            GDH.create_folder(options)

        # upload file, if finished it move to finished folder
        upload_opt = {
            'dir_upload': "E:\\projects\\god_follow_210612\\app\\data\\backup\\upload_needed",
            'dir_finished': "E:\\projects\\god_follow_210612\\app\\data\\backup\\finished",
            'parent_id': download_needed_dir_id
        }
        GDH.upload(upload_opt)

    elif mode == 'download':

        # check today dir exist, if not create it
        file_list = GDH.get_drive_file_list_all()

        flag_date_dir_exist = 0
        download_needed_dir_id = ''
        finished_dir_id = ''

        for file_e in file_list:
            file_id = file_e[0]
            file_name = file_e[1]
            file_mime = file_e[2]
            file_parent = file_e[3]

            if file_name == date and file_mime == 'application/vnd.google-apps.folder':
                flag_date_dir_exist = 1

                # Get download_needed folder id
                for file_e2 in file_list:
                    if file_e2[1] == 'download_needed' and file_e2[2] == 'application/vnd.google-apps.folder' and\
                            file_e2[3][0] == file_id:
                        download_needed_dir_id = file_e2[0]
                    elif file_e2[1] == 'finished' and file_e2[2] == 'application/vnd.google-apps.folder' and\
                            file_e2[3][0] == file_id:
                        finished_dir_id = file_e2[0]

                break

        # download file, if finished it move to finished folder
        if flag_date_dir_exist:
            download_opt = {
                'dir_download': "E:\\projects\\god_follow_200819\\app\\data\\download",
                'download_needed_dir_id': download_needed_dir_id,
                'finished_dir_id': finished_dir_id
            }
            GDH.download(download_opt)