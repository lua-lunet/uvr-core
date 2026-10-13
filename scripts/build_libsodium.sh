#!/usr/bin/env bash
# Build libsodium from source for the PAXE codec's static link.
#
# Why this exists, in the codec's own terms: the codec requires a libsodium
# with the hardware AES-256-GCM path, and it refuses to run without one
# rather than degrading. Some distro builds ship without it: Ubuntu 24.04
# and Debian trixie on arm64 carry libsodium 1.0.18, whose
# crypto_aead_aes256gcm_is_available() answers 0 because the build has no
# ARM Crypto Extensions path. A host like that refuses at init, which is the
# law working, not a defect to be worked around in the tests.
#
# So the build machine gets a sodium that has the path, pinned by SHA256,
# static-only (no shared object to resolve at link time, nothing to deploy).
# macOS does not need this: Homebrew carries 1.0.22 with the path. The Linux
# lane uses this instead of the distro package for exactly that reason.
#
# The result is announced through PAXE_SODIUM_LIB_DIR, which the codec's
# build script consults before it searches the system, so nothing else has
# to be configured.
#
# Usage: SODIUM_PREFIX=/tmp/libsodium scripts/build_libsodium.sh
set -euo pipefail

# 1.0.22 is the release that has both properties this needs: the aarch64
# build carries the ARM Crypto Extensions path, and its sources compile
# under the arm64 GCC in current runners. 1.0.21 fails to build there
# (crypto_ipcrypt/ipcrypt_armcrypto.c reinterprets an int8x16_t as uint8x16_t,
# which GCC rejects, upstream having fixed the signedness in 1.0.22). It is
# also what the macOS lane already gets from Homebrew, so both lanes agree.
#
# The digest is of the tarball fetched from the URL below and is verified on
# every run. libsodium publishes no checksum file at its release index, so
# this pin is recorded from the download itself rather than from upstream.
VERSION="1.0.22"
SHA256="adbdd8f16149e81ac6078a03aca6fc03b592b89ef7b5ed83841c086191be3349"
TARBALL="libsodium-${VERSION}.tar.gz"
URL="https://download.libsodium.org/libsodium/releases/${TARBALL}"
PREFIX="${SODIUM_PREFIX:-/tmp/libsodium}"
WORKDIR="${SODIUM_WORKDIR:-/tmp/libsodium-build}"
JOBS="${NPROC:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)}"

echo "building libsodium ${VERSION} from source into ${PREFIX}"

mkdir -p "${WORKDIR}"
cd "${WORKDIR}"

if [[ ! -f "${TARBALL}.verified" ]]; then
	curl --max-time 120 -fsSL -o "${TARBALL}" "${URL}"
	echo "${SHA256}  ${TARBALL}" | sha256sum -c -
	touch "${TARBALL}.verified"
else
	echo "  the tarball is already verified"
fi

if [[ ! -d "libsodium-${VERSION}" ]]; then
	tar xzf "${TARBALL}"
fi

cd "libsodium-${VERSION}"

# Static only: a shared libsodium beside the archive is what makes a
# "static" link resolve to the shared object anyway.
./configure --enable-static --disable-shared --prefix="${PREFIX}" >/dev/null
make -j"${JOBS}" >/dev/null
make install >/dev/null

if [[ ! -f "${PREFIX}/lib/libsodium.a" ]]; then
	echo "the build finished without producing ${PREFIX}/lib/libsodium.a" >&2
	exit 1
fi

echo "  archive: ${PREFIX}/lib/libsodium.a"
echo "  export PAXE_SODIUM_LIB_DIR=${PREFIX}/lib before the cargo step that links it"
