#!/usr/bin/env bash

set -e

if [ "$TRAVIS_PULL_REQUEST" == 'false' ] && [ ! -z "$TRAVIS_TAG" ]; then
  echo "Installing keyrings"
  mkdir -p $GPG_DIR
  echo $GPG_PUBLIC_KEYS | base64 --decode >> ${GPG_DIR}/pubring.gpg
  echo $GPG_SECRET_KEYS | base64 --decode >> ${GPG_DIR}/secring.gpg
fi