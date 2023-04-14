#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_apollo_io import SourceApolloIo

if __name__ == "__main__":
    source = SourceApolloIo()
    launch(source, sys.argv[1:])
