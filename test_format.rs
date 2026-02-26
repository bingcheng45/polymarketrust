use ethers::types::Address;

fn main() {
    let addr = Address::random();
    let debug_fmt = format!("{:?}", addr);
    let display_fmt = format!("{:?}", format!("{:?}", addr));
    println!("Debug: {}", debug_fmt);
    println!("Display: {}", display_fmt);
}
