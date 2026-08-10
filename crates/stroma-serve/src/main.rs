//! `stroma-serve` — thin binary over the serving library; `stroma serve` runs the same code.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    stromadb_serve::run(&args);
}
