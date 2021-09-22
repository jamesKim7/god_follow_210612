
import sys
import os
import json
import subprocess
from datetime import datetime as dt

from app.module import mysqldump_handler
from app.module import google_drive_handler


def main_backup(table_list):
    root_dir = os.getcwd()

    MDH = mysqldump_handler.MysqlDumpHandler()
    GDH = google_drive_handler.GoogleDriveHandler()

    MDH.get_config()

    # table list input 값으로
    options = {
        'table_list': table_list
    }
    MDH.backup_table(options)

    os.chdir(root_dir)

    # google drive upload 시작
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
                    if file_e2[1] == 'download_needed' and file_e2[2] == 'application/vnd.google-apps.folder' and \
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
            file_e = GDH.create_folder(options)
            download_needed_dir_id = file_e['id']

            options = {
                'parent_id': new_folder_id,
                'file_name': 'finished'
            }
            GDH.create_folder(options)

        # upload file, if finished it move to finished folder
        upload_opt = {
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
                    if file_e2[1] == 'download_needed' and file_e2[2] == 'application/vnd.google-apps.folder' and \
                            file_e2[3][0] == file_id:
                        download_needed_dir_id = file_e2[0]
                    elif file_e2[1] == 'finished' and file_e2[2] == 'application/vnd.google-apps.folder' and \
                            file_e2[3][0] == file_id:
                        finished_dir_id = file_e2[0]

                break

        # download file, if finished it move to finished folder
        if flag_date_dir_exist:
            download_opt = {
                'download_needed_dir_id': download_needed_dir_id,
                'finished_dir_id': finished_dir_id
            }
            GDH.download(download_opt)


if __name__ == '__main__':
    pass