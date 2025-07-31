import sys
import os
from cx_Freeze import setup, Executable

base_dir = os.path.dirname(__file__)

build_exe_options = {
    "packages": ["asyncio", "xlwings", "asyncua"],
    "includes": [],
    "excludes": ["tkinter", "unittest", "xml", "http", "email", "pydoc", "setuptools", "distutils"],
    "include_files": [
        os.path.join(base_dir, "config.txt"),
        os.path.join(base_dir, "OPC_UA_Data.xlsx")
    ],
    "optimize": 1,
    "build_exe": "dist",
}

base = None
if sys.platform == "win32":
    pass

setup(
    name="OPC_UA_Excel_Client",
    version="0.1",
    description="OPC UA Client for Excel Data Synchronization",
    options={"build_exe": build_exe_options},
    executables=[Executable(
        os.path.join(base_dir, "ua_excel_client.py"),
        base=base,
        target_name="ua_excel_client.exe"
    )]
)