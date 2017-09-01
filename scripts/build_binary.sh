#!/bin/bash
# build binary for linux, osx and windows and compress them 
# compressed binaries can be found in /build directory  

rm -f build/mgodatagen*
env GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o build/mgodatagen
tar -czvf build/mgodatagen_linux_x86_64.tar.gz build/mgodatagen
rm -f build/mgodatagen

env GOOS=darwin GOARCH=amd64 go build -ldflags="-w -s" -o build/mgodatagen
tar -czvf build/mgodatagen_macOSX_x86_64.tar.gz build/mgodatagen 
rm -f build/mgodatagen 

env GOOS=windows GOARCH=amd64 go build -ldflags="-w -s" -o build/mgodatagen.exe
zip build/mgodatagen_windows_x86_64.zip build/mgodatagen.exe 
rm -f build/mgodatagen.exe
