#!/usr/bin/env bash

echo "Before install - OS is $TRAVIS_OS_NAME"

if [[ $TRAVIS_OS_NAME = 'osx' ]]; then
    echo "Updating homebrew"
    brew update
    echo "Installing and starting mongodb"
    brew install mongodb
    sudo mkdir -p /data/db
    brew services start mongodb
fi
