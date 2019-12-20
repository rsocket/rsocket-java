#!/usr/bin/env bash

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then

    echo -e "Building PR #$TRAVIS_PULL_REQUEST [$TRAVIS_PULL_REQUEST_SLUG/$TRAVIS_PULL_REQUEST_BRANCH => $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH]"
    ./gradlew build

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$bintrayUser" != "" ] && [ "$TRAVIS_BRANCH" == "develop" ] ; then

    echo -e "Building Develop Snapshot $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH/$TRAVIS_BUILD_NUMBER"
    ./gradlew \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PversionSuffix="-SNAPSHOT" \
        -PbuildNumber="$TRAVIS_BUILD_NUMBER" \
        build artifactoryPublish --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$bintrayUser" != "" ] ; then

    echo -e "Building Branch Snapshot $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH/$TRAVIS_BUILD_NUMBER"
    ./gradlew \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PversionSuffix="-${TRAVIS_BRANCH//\//-}-SNAPSHOT" \
        -PbuildNumber="$TRAVIS_BUILD_NUMBER" \
        build artifactoryPublish --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ] && [ "$bintrayUser" != "" ] ; then

    echo -e "Building Tag $TRAVIS_REPO_SLUG/$TRAVIS_TAG"
    ./gradlew \
        -Pversion="$TRAVIS_TAG" \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PbuildNumber="$TRAVIS_BUILD_NUMBER" \
        build bintrayUpload --stacktrace

else

    echo -e "Building $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH"
    ./gradlew build

fi

