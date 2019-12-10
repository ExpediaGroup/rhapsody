#!/usr/bin/env bash

set -e

if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  if [ ! -z "$TRAVIS_TAG" ]; then
    echo "Deploying release"
    gpg --version
    gpg --import ${GPG_DIR}/gpg.asc
    mvn deploy --settings travis/mvn-settings.xml -B -U -P oss-release -DskipTests=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
  else
    echo "Deploying snapshot"
    mvn deploy --settings travis/mvn-settings.xml -B -U -P oss-release -DskipTests=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
  fi
fi