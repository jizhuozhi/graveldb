fn main() {
    // Link against pre-built libgraveldb
    // Adjust this path to where your libgraveldb_lib.a is built
    println!("cargo:rustc-link-search=native=../../build");
    println!("cargo:rustc-link-lib=static=graveldb_lib");
    println!("cargo:rustc-link-lib=pthread");

    // On Linux with io_uring support
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=uring");

    // Rebuild if the C library changes
    println!("cargo:rerun-if-changed=../../include/graveldb.h");
    println!("cargo:rerun-if-changed=../../build/libgraveldb_lib.a");
}
