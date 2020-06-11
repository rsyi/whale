from PyInstaller.utils.hooks import collect_data_files, copy_metadata

datas = copy_metadata('pyhive')
hiddenimports = ['pyhive.sqlalchemy_presto']

