#!/usr/bin/env bash

set -e

if [ "$TRAVIS_PULL_REQUEST" == 'false' ] && [ ! -z "$TRAVIS_TAG" ]; then
  echo "Installing keyrings"
  mkdir -p $GPG_DIR
  openssl aes-256-cbc -K $encrypted_cd1337d2ab17_key -iv $encrypted_cd1337d2ab17_iv -in travis/gpg.asc.enc -out ${GPG_DIR}/gpg.asc -d
  echo $GPG_PUBLIC_KEYS | base64 --decode >> ${GPG_DIR}/pubring.gpg
  echo $GPG_SECRET_KEYS | base64 --decode >> ${GPG_DIR}/secring.gpg
fi