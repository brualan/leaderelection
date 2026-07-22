// The core module deliberately has no external dependencies: it is pure
// standard library, including its tests. Database drivers live in the
// ./postgres and ./mysql submodules so that importing one does not drag in
// the other.
module github.com/brualan/leaderelection

go 1.23.0
