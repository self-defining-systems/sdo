# Shared by build.sh and up.sh (source it).
# tree_hash DIR prints the first 10 hex chars of a hash over the names and contents
# of the files under DIR. Good builds and patched (bad) builds are both tagged
# fuzzops/shop:<hash of their source> (the patched-image action computes the same
# hash), so tags look alike and carry no meaning.
tree_hash() {
  (cd "$1" && find . -type f | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-10)
}
