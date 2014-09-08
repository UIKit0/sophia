#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

go vet .
#golint
go build -o sophia
rm ./sophia
