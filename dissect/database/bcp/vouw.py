from dissect.database.bcp.bcp import BCP
from pathlib import Path

paths = [
    Path("/home/user/dissect/testdata/bcp/Table_1.dat"),
    Path("/home/user/dissect/testdata/bcp/Table_2.dat")
]

for path in paths:
    with BCP(path) as bcp:
        for value in bcp.values():
            print(value)
    print("-"*40)