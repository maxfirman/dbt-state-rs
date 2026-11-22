// `sqlx::migrate!` embeds the server's migrations (`MIGRATOR`); rebuild when
// one is added or changed.
fn main() {
    println!("cargo:rerun-if-changed=../server/migrations");
}
