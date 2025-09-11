#!/usr/bin/env bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# curvine package command
# ./build, debug mode
# ./build release, release mode.
FS_HOME="$(cd "`dirname "$0"`/.."; pwd)"

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed or not in PATH" >&2
    exit 1
fi

get_arch_name() {
    arch=$(uname -m)
    case $arch in
        x86_64)
            echo "x86_64"
            ;;
        i386 | i686)
            echo "x86_32"
            ;;
        aarch64 | arm64)
             echo "aarch_64"
            ;;
        armv7l | armv6l)
            echo "aarch_32"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

get_os_version() {
  if [ -f "/etc/os-release" ]; then
    id=$(grep -E '^ID=' /etc/os-release | cut -d= -f2- | tr -d '"')
    ver=$(grep ^VERSION_ID= /etc/os-release | cut -d '"' -f 2| cut -d '.' -f 1)
    echo $id$ver
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    echo "mac"
  else
    echo "unknown"
  fi
}

get_fuse_version() {
  if command -v fusermount3 > /dev/null 2>&1; then
      echo "fuse3"
  elif command -v fusermount > /dev/null 2>&1; then
      echo "fuse2"
  else
      echo ""  # No FUSE available
  fi
}

print_help() {
  echo "Usage: $0 [options]"
  echo
  echo "Options:"
  echo "  -p, --package PACKAGE  Package to build (can be specified multiple times, default: all)"
  echo "                        Available packages:"
  echo "                          - core: includes server, client, and cli"
  echo "                          - server: server component"
  echo "                          - client: client component"
  echo "                          - cli: command line interface"
  echo "                          - web: web interface"
  echo "                          - fuse: FUSE filesystem"
  echo "                          - java: Java SDK"
  echo "                          - python: Python SDK"
  echo "                          - tests: test suite and benchmarks"
  echo "                          - object: S3 object gateway"
  echo "                          - all: all packages"
  echo
  echo "  -u, --ufs TYPE        UFS storage type (can be specified multiple times, default: opendal-s3)"
  echo "                        Available types:"
  echo "                          - s3: AWS S3 native SDK"
  echo "                          - opendal-s3: OpenDAL S3"
  echo "                          - opendal-oss: OpenDAL OSS"
  echo "                          - opendal-azblob: OpenDAL Azure Blob"
  echo "                          - opendal-gcs: OpenDAL GCS"
  echo
  echo "  -d, --debug           Build in debug mode (default: release mode)"
  echo "  -z, --zip             Create zip archive"
  echo "  -h, --help            Show this help message"
  echo
  echo "Examples:"
  echo "  $0                                      # Build all packages in release mode with opendal-s3"
  echo "  $0 --package core --ufs s3             # Build core packages with server, client and cli"
  echo "  $0 -p web --package fuse --debug       # Build web and fuse in debug mode"
  echo "  $0 --package all --ufs opendal-s3 -z   # Build all packages with OpenDAL S3 and create zip"
}

# Create a version file.
GIT_VERSION="unknown"
if command -v git &> /dev/null && git rev-parse --git-dir &> /dev/null; then
    GIT_VERSION=$(git rev-parse HEAD)
fi

# Get the necessary environment parameters
ARCH_NAME=$(get_arch_name)
OS_VERSION=$(get_os_version)
FUSE_VERSION=$(get_fuse_version)
CURVINE_VERSION=$(grep '^version =' "$FS_HOME/Cargo.toml" | sed 's/^version = "\(.*\)"/\1/')

# Package Directory
DIST_DIR="$FS_HOME/build/dist/"
DIST_ZIP=curvine-${CURVINE_VERSION}-${ARCH_NAME}-${OS_VERSION}.zip

# Process command parameters
PROFILE="--release"
declare -a PACKAGES=("all")  # Default to build all packages
declare -a UFS_TYPES=("s3")  # Default UFS type
CRATE_ZIP=""

# Parse command line arguments
TEMP=$(getopt -o p:u:dzhv --long package:,ufs:,debug,zip,help -n "$0" -- "$@")
if [ $? != 0 ] ; then print_help ; exit 1 ; fi

eval set -- "$TEMP"

while true ; do
  case "$1" in
    -p|--package)
      # If this is the first -p argument, clear the default "all"
      if [ ${#PACKAGES[@]} -eq 1 ] && [ "${PACKAGES[0]}" = "all" ]; then
        PACKAGES=()
      fi
      PACKAGES+=("$2")
      shift 2
      ;;
    -u|--ufs)
      UFS_TYPES+=("$2")
      shift 2
      ;;
    -d|--debug)
      PROFILE=""
      shift
      ;;
    -z|--zip)
      CRATE_ZIP="zip"
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      print_help
      exit 1
      ;;
  esac
done

# Check if "all" is specified along with other packages
for pkg in "${PACKAGES[@]}"; do
  if [ "$pkg" = "all" ] && [ ${#PACKAGES[@]} -gt 1 ]; then
    echo "Error: 'all' cannot be combined with other packages" >&2
    exit 1
  fi
done

# Handle core package
if [[ " ${PACKAGES[@]} " =~ " core " ]]; then
  # Replace core with its components
  PACKAGES=("${PACKAGES[@]/core/}")
  PACKAGES+=("server" "client" "cli")
fi

# Export UFS types as comma-separated string
CURVINE_UFS_TYPE=$(IFS=,; echo "${UFS_TYPES[*]}")

# Create necessary directories
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/conf
mkdir -p "$DIST_DIR"/bin
mkdir -p "$DIST_DIR"/lib


# Copy configuration files and bin
cp "$FS_HOME"/etc/* "$DIST_DIR"/conf
cp "$FS_HOME"/build/bin/* "$DIST_DIR"/bin
chmod +x "$DIST_DIR"/bin/*

# Write version file
echo "commit=$GIT_VERSION" > "$DIST_DIR"/build-version
echo "os=${OS_VERSION}_$ARCH_NAME" >> "$DIST_DIR"/build-version
echo "fuse=${FUSE_VERSION:-none}" >> "$DIST_DIR"/build-version
echo "version=$CURVINE_VERSION" >> "$DIST_DIR"/build-version
echo "ufs_types=${CURVINE_UFS_TYPE}" >> "$DIST_DIR"/build-version


# Check if a package should be built
should_build_package() {
  local package=$1
  if [[ " ${PACKAGES[@]} " =~ " all " ]]; then
    return 0
  fi
  if [[ " ${PACKAGES[@]} " =~ " $package " ]]; then
    return 0
  fi
  return 1
}

# Collect all rust packages to build
declare -a RUST_BUILD_ARGS=()
declare -a COPY_TARGETS=()

# Add required packages
if should_build_package "server"; then
  RUST_BUILD_ARGS+=("-p" "curvine-server")
  COPY_TARGETS+=("curvine-server")
fi

if should_build_package "client"; then
  RUST_BUILD_ARGS+=("-p" "curvine-client")
  # COPY_TARGETS+=("curvine-client")
fi

if should_build_package "cli"; then
  RUST_BUILD_ARGS+=("-p" "curvine-cli")
  COPY_TARGETS+=("curvine-cli")
fi

# Add optional rust packages
if should_build_package "fuse" && [ -n "$FUSE_VERSION" ]; then
  RUST_BUILD_ARGS+=("-p" "curvine-fuse")
  COPY_TARGETS+=("curvine-fuse")
fi

if should_build_package "java"; then
  RUST_BUILD_ARGS+=("-p" "curvine-libsdk")
fi

if should_build_package "tests"; then
  RUST_BUILD_ARGS+=("-p" "curvine-tests")
  COPY_TARGETS+=("curvine-bench")
fi

# Add S3 object gateway package
if should_build_package "object" || [[ " ${PACKAGES[@]} " =~ " all " ]]; then
  RUST_BUILD_ARGS+=("-p" "curvine-s3-gateway")
  COPY_TARGETS+=("curvine-s3-gateway")
fi

# Base command
cmd="cargo build $PROFILE"

# Add package arguments if any
if [ ${#RUST_BUILD_ARGS[@]} -gt 0 ]; then
  cmd="$cmd ${RUST_BUILD_ARGS[@]}"
fi

# Collect all features
declare -a FEATURES=()

# Check FUSE availability if needed
if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-fuse " ]] || [[ " ${PACKAGES[@]} " =~ " all " ]]; then
  if [ -z "$FUSE_VERSION" ]; then
    echo "Warn: FUSE package requested but FUSE is not available on this system" >&2
  fi
fi

# Add features based on what we're actually building
if [ ${#RUST_BUILD_ARGS[@]} -gt 0 ]; then
  # Add FUSE features if we're building fuse
  if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-fuse " ]]; then
    FEATURES+=("curvine-fuse/$FUSE_VERSION")
  fi

  # Add UFS features if we're building client
  if [[ " ${RUST_BUILD_ARGS[@]} " =~ " -p curvine-client " ]]; then
    for ufs in "${UFS_TYPES[@]}"; do
      case $ufs in
        s3)
          # Use s3 feature for AWS SDK implementation
          FEATURES+=("curvine-client/s3")
          ;;
        *)
          FEATURES+=("curvine-client/$ufs")
          ;;
      esac
    done
  fi
else
  # If building all packages, add all relevant features
  FEATURES+=("curvine-fuse/$FUSE_VERSION")  # FUSE check already done above
  for ufs in "${UFS_TYPES[@]}"; do
    case $ufs in
      s3)
        # Use s3-native feature for AWS SDK implementation
        FEATURES+=("curvine-client/s3")
        ;;
      *)
        FEATURES+=("curvine-client/$ufs")
        ;;
    esac
  done
fi

# Add features to command if any
if [ ${#FEATURES[@]} -gt 0 ]; then
  # Join features with comma
  IFS=, eval 'FEATURE_LIST="${FEATURES[*]}"'
  cmd="$cmd --no-default-features --features $FEATURE_LIST"
fi

# Skip cargo build if only building web module
if [ ${#PACKAGES[@]} -eq 1 ] && [ "${PACKAGES[0]}" = "web" ]; then
  echo "Only building web module, skipping cargo build..."
else
  echo "Building crates with command: $cmd"
  eval "$cmd"

  if [ $? -ne 0 ]; then
    echo "Cargo build failed. Exiting..."
    exit 1
  fi
fi

# Copy all built binaries
for target in "${COPY_TARGETS[@]}"; do
  cp -f "$FS_HOME"/target/${PROFILE#--}/${target} "$DIST_DIR"/lib
done

# Build optional non-rust packages
if should_build_package "web"; then
  echo "Building WebUI..."
  cd "$FS_HOME"/curvine-web/webui
  npm install
  npm run build
  mv "$FS_HOME"/curvine-web/webui/dist "$DIST_DIR"/webui
fi

if should_build_package "java"; then
  mkdir -p "$FS_HOME"/curvine-libsdk/java/native
  
  # Handle java native library
  if [ -e "$FS_HOME/target/${PROFILE#--}/curvine_libsdk.dll" ]; then
    cp -f "$FS_HOME/target/${PROFILE#--}/curvine_libsdk.dll" "$FS_HOME/curvine-libsdk/java/native/curvine_libsdk.dll"
  elif [ -e "$FS_HOME/target/${PROFILE#--}/libcurvine_libsdk.so" ]; then
    cp -f "$FS_HOME/target/${PROFILE#--}/libcurvine_libsdk.so" "$FS_HOME/curvine-libsdk/java/native/libcurvine_libsdk_${OS_VERSION}_$ARCH_NAME.so"
  fi

  # Build curvine-hadoop java package
  echo "Building curvine-hadoop Java package..."
  cd "$FS_HOME"/curvine-libsdk/java
  mvn protobuf:compile package -DskipTests -P${PROFILE#--}
  if [ $? -ne 0 ]; then
    echo "curvine-hadoop Java build failed. Exiting..."
    exit 1
  fi
  cp "$FS_HOME"/curvine-libsdk/java/target/curvine-hadoop-*.jar "$DIST_DIR"/lib
fi

# Build OSS sync worker Java component (separate from main java package)
if should_build_package "server" || should_build_package "all"; then
  echo "Building OSS sync worker Java component..."
  cd "$FS_HOME"/curvine-server/java
  mvn clean compile package -DskipTests -q
  if [ $? -ne 0 ]; then
    echo "OSS sync worker Java build failed. Exiting..."
    exit 1
  fi
  cp "$FS_HOME"/curvine-server/java/target/curvine-oss-sync-*.jar "$DIST_DIR"/lib/
  echo "OSS sync worker JAR copied to $DIST_DIR/lib/"
fi

# create zip
cd "$DIST_DIR"
if [[ ${CRATE_ZIP} = "zip" ]]; then
  zip -m -r "$DIST_ZIP" *
  echo "build success, file: $DIST_DIR/$DIST_ZIP"
else
    echo "build success, dir: $DIST_DIR"
fi