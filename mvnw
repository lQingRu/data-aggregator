#!/usr/bin/env sh
set -eu

BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WRAPPER_PROPERTIES="$BASE_DIR/.mvn/wrapper/maven-wrapper.properties"
MAVEN_USER_HOME="${MAVEN_USER_HOME:-"$HOME/.m2"}"
DIST_URL=$(sed -n 's/^distributionUrl=//p' "$WRAPPER_PROPERTIES")
DIST_NAME=$(basename "$DIST_URL" -bin.tar.gz)
DIST_DIR="$MAVEN_USER_HOME/wrapper/dists/$DIST_NAME"
MAVEN_BIN="$DIST_DIR/$DIST_NAME/bin/mvn"

if [ ! -x "$MAVEN_BIN" ]; then
    mkdir -p "$DIST_DIR"
    ARCHIVE="$DIST_DIR/$DIST_NAME-bin.tar.gz"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$DIST_URL" -o "$ARCHIVE"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$DIST_URL" -O "$ARCHIVE"
    else
        echo "curl or wget is required to download Maven." >&2
        exit 1
    fi
    tar -xzf "$ARCHIVE" -C "$DIST_DIR"
fi

exec "$MAVEN_BIN" "$@"
