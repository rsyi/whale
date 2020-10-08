from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata('snowflake-sqlalchemy')

hiddenimports = [
    'snowflake',
    'snowflake.sqlalchemy'
    ]
