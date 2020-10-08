from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata('pyhive')
hiddenimports = ['pyhive.sqlalchemy_presto']
